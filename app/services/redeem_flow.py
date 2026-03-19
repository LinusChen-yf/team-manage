"""
兑换流程服务 (Redeem Flow Service)
协调兑换码验证, Team 选择和加入 Team 的完整流程
"""
import logging
import asyncio
import traceback
from collections import defaultdict
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.redemption import RedemptionService
from app.services.team import TeamService
from app.services.warranty import warranty_service
from app.services.notification import notification_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

# 全局兑换锁: 针对 code 进行加锁，防止同一个码并发请求
_code_locks = defaultdict(asyncio.Lock)
# 全局 Team 锁: 针对 Team 进行加锁，防止并发拉人导致的人数状态不同步
_team_locks = defaultdict(asyncio.Lock)
# 全局邮箱锁: 针对 email 进行加锁，防止同一邮箱并发兑换被分到同一个 Team
_email_locks = defaultdict(asyncio.Lock)
# 单次兑换流程中最多尝试的 Team 数量上限，防止可用 Team 极多时产生死循环或过长等待
_MAX_TEAM_RETRY_LIMIT = 50
# 兑换码锁等待超时（秒）：超时说明系统繁忙，直接返回错误
_CODE_LOCK_TIMEOUT = 60
# Team 锁等待超时（秒）：超时则排除该 Team 尝试下一个
_TEAM_LOCK_TIMEOUT = 30


class RedeemFlowService:
    """兑换流程场景服务类"""

    def __init__(self):
        """初始化兑换流程服务"""
        from app.services.chatgpt import chatgpt_service
        self.redemption_service = RedemptionService()
        self.warranty_service = warranty_service
        self.team_service = TeamService()
        self.chatgpt_service = chatgpt_service

    async def verify_code_and_get_teams(
        self,
        code: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        验证兑换码并返回可用 Team 列表
        针对 aiosqlite 进行优化，避免 greenlet_spawn 报错
        """
        try:
            # 1. 验证兑换码
            validate_result = await self.redemption_service.validate_code(code, db_session)

            if not validate_result["success"]:
                return {
                    "success": False,
                    "valid": False,
                    "reason": None,
                    "teams": [],
                    "error": validate_result["error"]
                }
            
            # 如果是已经标记为过期了
            if not validate_result["valid"] and validate_result.get("reason") == "兑换码已过期 (超过首次兑换截止时间)":
                try:
                    await db_session.commit()
                except:
                    pass

            if not validate_result["valid"]:
                return {
                    "success": True,
                    "valid": False,
                    "reason": validate_result["reason"],
                    "teams": [],
                    "error": None
                }

            # 2. 获取可用 Team 列表
            teams_result = await self.team_service.get_available_teams(db_session)

            if not teams_result["success"]:
                return {
                    "success": False,
                    "valid": True,
                    "reason": "兑换码有效",
                    "teams": [],
                    "error": teams_result["error"]
                }

            logger.info(f"验证兑换码成功: {code}, 可用 Team 数量: {len(teams_result['teams'])}")

            return {
                "success": True,
                "valid": True,
                "reason": "兑换码有效",
                "teams": teams_result["teams"],
                "error": None
            }

        except Exception as e:
            logger.error(f"验证兑换码并获取 Team 列表失败: {e}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "valid": False,
                "reason": None,
                "teams": [],
                "error": f"验证失败: {str(e)}"
            }

    async def select_team_auto(
        self,
        db_session: AsyncSession,
        exclude_team_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        自动选择一个可用的 Team
        """
        try:
            # 查找所有 active 且未满的 Team
            stmt = select(Team).where(
                Team.status == "active",
                Team.current_members < Team.max_members
            )
            
            if exclude_team_ids:
                stmt = stmt.where(Team.id.not_in(exclude_team_ids))
            
            # 优先选择人数最少的 Team (负载均衡)
            stmt = stmt.order_by(Team.current_members.asc(), Team.created_at.desc())
            
            result = await db_session.execute(stmt)
            team = result.scalars().first()

            if not team:
                reason = "没有可用的 Team"
                if exclude_team_ids:
                    reason = "您已加入所有可用 Team"
                return {
                    "success": False,
                    "team_id": None,
                    "error": reason
                }

            logger.info(f"自动选择 Team: {team.id}")

            return {
                "success": True,
                "team_id": team.id,
                "error": None
            }

        except Exception as e:
            logger.error(f"自动选择 Team 失败: {e}")
            return {
                "success": False,
                "team_id": None,
                "error": f"自动选择 Team 失败: {str(e)}"
            }

    async def redeem_and_join_team(
        self,
        email: str,
        code: str,
        team_id: Optional[int],
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        完整的兑换流程 (带事务和并发控制)
        send_invite 成功后立即返回前端，后台异步验证成员是否真实加入（防止虚假成功）。
        虚假成功时后台自动回滚并标记 Team 为 error，不影响前端响应速度。

        锁嵌套顺序: email → code → team
        """
        last_error = "未知错误"
        # 用于跟踪因失败被排除的 Team（使用 set 保证 O(1) 查找）
        excluded_team_ids: set = set()
        current_target_team_id = team_id

        # 邮箱级别锁 (最外层)，防止同一邮箱并发兑换请求同时进入流程
        async with _email_locks[email]:
            # 针对 code 加锁，带 60s 超时，防止同一个码并发进入兑换
            code_lock = _code_locks[code]
            try:
                await asyncio.wait_for(code_lock.acquire(), timeout=_CODE_LOCK_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"获取兑换码锁超时 (code={code}, email={email})")
                return {"success": False, "error": "系统繁忙，请稍后再试"}

            try:
                # 查询该邮箱已有的活跃/满员 Team，防止重复加入同一 Team
                stmt = select(RedemptionRecord.team_id).where(RedemptionRecord.email == email)
                records = await db_session.execute(stmt)
                email_record_team_ids = {r[0] for r in records.fetchall()}

                if email_record_team_ids:
                    stmt2 = select(Team.id).where(
                        Team.id.in_(email_record_team_ids),
                        Team.status.in_(["active", "full"])
                    )
                    result2 = await db_session.execute(stmt2)
                    email_existing_team_ids = {r[0] for r in result2.fetchall()}
                else:
                    email_existing_team_ids = set()

                logger.info(f"邮箱 {email} 已有的活跃 Team: {email_existing_team_ids}")

                # 手动指定 team_id 时，检查邮箱是否已在该 Team 中
                if team_id and team_id in email_existing_team_ids:
                    return {"success": False, "error": "该邮箱已在该 Team 中，请勿重复兑换"}

                # 将邮箱已有的 Team 加入排除列表，避免自动选中
                excluded_team_ids.update(email_existing_team_ids)

                # 动态计算当前可用 Team 总数作为最大重试次数，确保遍历所有可用 Team
                count_stmt = select(func.count()).select_from(Team).where(
                    Team.status == "active",
                    Team.current_members < Team.max_members
                )
                count_res = await db_session.execute(count_stmt)
                available_count = count_res.scalar() or 0
                max_retries = max(1, min(_MAX_TEAM_RETRY_LIMIT, available_count))

                for attempt in range(max_retries):
                    logger.info(f"兑换尝试 {attempt + 1}/{max_retries} (Code: {code}, Email: {email})")

                    team_id_final = None
                    _skip_to_next_team = False

                    try:
                        # 确定目标 Team (初选)
                        team_id_final = current_target_team_id
                        if not team_id_final:
                            select_res = await self.select_team_auto(
                                db_session,
                                exclude_team_ids=list(excluded_team_ids) if excluded_team_ids else None
                            )
                            if not select_res["success"]:
                                return {"success": False, "error": "当前没有可用 Team，请稍后再试"}
                            team_id_final = select_res["team_id"]
                            current_target_team_id = team_id_final

                        # 使用 Team 锁序列化对该账户的操作，带 30s 超时防止无限阻塞
                        team_lock = _team_locks[team_id_final]
                        try:
                            await asyncio.wait_for(team_lock.acquire(), timeout=_TEAM_LOCK_TIMEOUT)
                        except asyncio.TimeoutError:
                            logger.warning(
                                f"获取 Team {team_id_final} 锁超时 (code={code}, email={email})，尝试下一个 Team"
                            )
                            excluded_team_ids.add(team_id_final)
                            current_target_team_id = None
                            last_error = "系统繁忙，请稍后再试"
                            if attempt < max_retries - 1:
                                continue
                            break

                        try:
                            logger.info(f"锁定 Team {team_id_final} 执行核心兑换步骤 (尝试 {attempt+1})")

                            # 重置 Session 状态，确保没有残留事务（应对上一轮迭代可能的失败）
                            if db_session.in_transaction():
                                await db_session.rollback()
                            elif db_session.is_active:
                                await db_session.rollback()

                            # 1. 前置同步：拉人前确保人数状态绝对实时 (耗时操作)
                            await self.team_service.sync_team_info(team_id_final, db_session)

                            # 2. 核心校验 (开启短事务)
                            if not db_session.in_transaction():
                                await db_session.begin()

                            try:
                                # 1. 验证和锁定码
                                stmt = select(RedemptionCode).where(RedemptionCode.code == code).with_for_update()
                                res = await db_session.execute(stmt)
                                rc = res.scalar_one_or_none()

                                if not rc:
                                    await db_session.rollback()
                                    return {"success": False, "error": "兑换码不存在"}

                                if rc.status not in ["unused", "warranty_active"]:
                                    if rc.status == "used":
                                        warranty_check = await self.warranty_service.validate_warranty_reuse(
                                            db_session, code, email
                                        )
                                        if not warranty_check.get("can_reuse"):
                                            await db_session.rollback()
                                            return {"success": False, "error": warranty_check.get("reason") or "兑换码已使用"}
                                    else:
                                        await db_session.rollback()
                                        return {"success": False, "error": f"兑换码状态无效: {rc.status}"}

                                # 2. 锁定并校验 Team
                                stmt = select(Team).where(Team.id == team_id_final).with_for_update()
                                res = await db_session.execute(stmt)
                                target_team = res.scalar_one_or_none()

                                if not target_team or target_team.status != "active":
                                    raise Exception(f"目标 Team {team_id_final} 不可用 ({target_team.status if target_team else 'None'})")

                                if target_team.current_members >= target_team.max_members:
                                    target_team.status = "full"
                                    raise Exception("该 Team 已满, 请选择其他 Team 尝试")

                                # 提取必要信息后立即提交，释放 DB 锁以进行耗时的 API 调用
                                account_id_to_use = target_team.account_id
                                team_email_to_use = target_team.email
                                await db_session.commit()
                            except Exception as e:
                                if db_session.in_transaction():
                                    await db_session.rollback()
                                raise e

                            # 3. 执行 API 邀请 (耗时操作，放事务外)
                            # 必须重新加载 target_team
                            res = await db_session.execute(select(Team).where(Team.id == team_id_final))
                            target_team = res.scalar_one_or_none()

                            access_token = await self.team_service.ensure_access_token(target_team, db_session)
                            if not access_token:
                                raise Exception("获取 Team 访问权限失败，账户状态异常")

                            invite_res = await self.chatgpt_service.send_invite(
                                access_token, account_id_to_use, email, db_session,
                                identifier=team_email_to_use
                            )

                            # 4. 后置处理与状态持久化 (第二次短事务)
                            if not db_session.in_transaction():
                                await db_session.begin()

                            try:
                                # 重新载入，确保状态最新
                                res = await db_session.execute(select(RedemptionCode).where(RedemptionCode.code == code).with_for_update())
                                rc = res.scalar_one_or_none()
                                res = await db_session.execute(select(Team).where(Team.id == team_id_final).with_for_update())
                                target_team = res.scalar_one_or_none()

                                if not invite_res["success"]:
                                    err = invite_res.get("error", "邀请失败")
                                    err_str = str(err).lower()
                                    if any(kw in err_str for kw in ["already in workspace", "already in team", "already a member"]):
                                        # 检查数据库是否已有该 email+team_id 的兑换记录
                                        existing_rec_stmt = select(RedemptionRecord).where(
                                            RedemptionRecord.email == email,
                                            RedemptionRecord.team_id == team_id_final
                                        )
                                        existing_rec_res = await db_session.execute(existing_rec_stmt)
                                        existing_rec = existing_rec_res.scalar_one_or_none()

                                        if existing_rec:
                                            logger.info(
                                                f"用户 {email} 已在 Team {team_id_final} 中（有兑换记录），视为兑换成功"
                                            )
                                            # 走下方的成功逻辑
                                        else:
                                            logger.warning(
                                                f"用户 {email} 已在 Team {team_id_final} 中（无兑换记录，可能是手动加入），"
                                                f"排除该 Team 重试"
                                            )
                                            await db_session.rollback()
                                            _skip_to_next_team = True
                                    else:
                                        if any(kw in err_str for kw in ["maximum number of seats", "full", "no seats"]):
                                            target_team.status = "full"
                                            await db_session.commit()
                                            raise Exception(f"该 Team 席位已满 (API Error: {err})")
                                        await db_session.rollback()
                                        raise Exception(err)

                                if not _skip_to_next_team:
                                    # 成功逻辑：先提交码和记录，再做同步验证
                                    rc.status = "used"
                                    rc.used_by_email = email
                                    rc.used_team_id = team_id_final
                                    rc.used_at = get_now()
                                    if rc.has_warranty:
                                        days = rc.warranty_days or 30
                                        rc.warranty_expires_at = get_now() + timedelta(days=days)

                                    record = RedemptionRecord(
                                        email=email,
                                        code=code,
                                        team_id=team_id_final,
                                        account_id=target_team.account_id,
                                        is_warranty_redemption=rc.has_warranty
                                    )
                                    db_session.add(record)
                                    target_team.current_members += 1
                                    if target_team.current_members >= target_team.max_members:
                                        target_team.status = "full"

                                    await db_session.commit()
                            except Exception as e:
                                if db_session.in_transaction():
                                    await db_session.rollback()
                                raise e
                        finally:
                            team_lock.release()

                        # 若 API 返回"已在 Team 中"且无兑换记录，排除该 Team 重试（不消耗兑换码）
                        if _skip_to_next_team:
                            excluded_team_ids.add(team_id_final)
                            current_target_team_id = None
                            last_error = f"Team {team_id_final} 已含该邮箱，尝试下一个"
                            if attempt < max_retries - 1:
                                continue
                            break

                        # 5. 后台异步验证：邀请成功后立即返回前端，后台确认成员真实加入
                        # 如果是虚假成功，后台自动回滚并标记 Team 为 error
                        asyncio.create_task(
                            self._background_verify_and_rollback(
                                email=email,
                                code=code,
                                team_id=team_id_final,
                                team_name=target_team.team_name,
                            )
                        )

                        return {
                            "success": True,
                            "message": "兑换成功！邀请链接已发送至您的邮箱，请及时查收。",
                            "team_info": {
                                "id": team_id_final,
                                "team_name": target_team.team_name,
                                "email": target_team.email,
                                "expires_at": target_team.expires_at.isoformat() if target_team.expires_at else None
                            }
                        }

                    except Exception as e:
                        last_error = str(e)
                        logger.error(f"兑换迭代失败 ({attempt+1}): {last_error}")

                        try:
                            if db_session.in_transaction():
                                await db_session.rollback()
                        except:
                            pass

                        # 判读是否中断重试
                        if any(kw in last_error for kw in ["不存在", "已使用", "已有正在使用", "质保已过期"]):
                            return {"success": False, "error": last_error}

                        # 将失败的 Team 加入排除列表，换一个 Team 继续尝试
                        if team_id_final:
                            excluded_team_ids.add(team_id_final)
                        current_target_team_id = None

                        # 判定是否需要永久标记为"满员"
                        if any(kw in last_error.lower() for kw in ["已满", "seats", "full"]):
                            try:
                                if not team_id:
                                    from sqlalchemy import update as sqlalchemy_update
                                    await db_session.execute(
                                        sqlalchemy_update(Team).where(Team.id == team_id_final).values(status="full")
                                    )
                                    await db_session.commit()
                            except:
                                pass

                        if attempt < max_retries - 1:
                            await asyncio.sleep(1.5)
                            continue

                # 所有可用 Team 均尝试失败
                if "系统繁忙" in last_error:
                    return {"success": False, "error": "系统繁忙，请稍后再试"}
                return {
                    "success": False,
                    "error": "当前没有可用 Team，请稍后再试"
                }

            finally:
                code_lock.release()

    async def _background_verify_and_rollback(
        self,
        email: str,
        code: str,
        team_id: int,
        team_name: str,
    ) -> None:
        """
        后台异步任务：验证邀请是否真实生效，如果是虚假成功则自动回滚并标记 Team 为 error。
        使用独立的数据库会话，避免与主流程共享会话冲突。
        """
        from app.database import AsyncSessionLocal
        _VERIFY_SLEEP_SECS = 3
        _VERIFY_MAX_RETRIES = 3

        async with AsyncSessionLocal() as bg_session:
            try:
                is_verified = False
                for v in range(_VERIFY_MAX_RETRIES):
                    await asyncio.sleep(_VERIFY_SLEEP_SECS)
                    sync_res = await self.team_service.sync_team_info(team_id, bg_session)
                    member_emails = [m.lower() for m in sync_res.get("member_emails", [])]
                    if email.lower() in member_emails:
                        is_verified = True
                        logger.info(f"[后台验证] Team {team_id} 同步验证成功 (第 {v+1} 次)，成员 {email} 已确认加入")
                        break
                    if v < _VERIFY_MAX_RETRIES - 1:
                        logger.warning(f"[后台验证] Team {team_id} 第 {v+1} 次未见成员 {email}，继续等待...")

                if is_verified:
                    await notification_service.check_and_notify_low_stock()
                    return

                # 虚假成功：回滚兑换码、记录、成员计数，并将该 Team 标记为 error（不可用）
                logger.error(
                    f'[后台验证] 检测到"虚假成功": Team {team_id} 邀请返回成功但 '
                    f'{_VERIFY_SLEEP_SECS * _VERIFY_MAX_RETRIES}s 后仍查不到成员 {email}，执行回滚'
                )

                if not bg_session.in_transaction():
                    await bg_session.begin()
                try:
                    # 回滚兑换码状态
                    res = await bg_session.execute(select(RedemptionCode).where(RedemptionCode.code == code).with_for_update())
                    rc_rb = res.scalar_one_or_none()
                    if rc_rb and rc_rb.status == "used" and rc_rb.used_team_id == team_id:
                        rc_rb.status = "unused"
                        rc_rb.used_by_email = None
                        rc_rb.used_team_id = None
                        rc_rb.used_at = None
                        rc_rb.warranty_expires_at = None
                        logger.warning(f"[后台验证] 已将兑换码 {code} 回滚为 unused（虚假成功补偿）")
                    elif rc_rb:
                        logger.warning(
                            f"[后台验证] 兑换码 {code} 状态已不符合预期 "
                            f"(status={rc_rb.status}, used_team_id={rc_rb.used_team_id})，可能已被并发修改，跳过回滚"
                        )

                    # 删除兑换记录
                    await bg_session.execute(
                        delete(RedemptionRecord).where(
                            RedemptionRecord.code == code,
                            RedemptionRecord.team_id == team_id,
                            RedemptionRecord.email == email
                        )
                    )

                    # 回滚 Team 成员计数并标记为 error
                    res = await bg_session.execute(select(Team).where(Team.id == team_id).with_for_update())
                    t_rb = res.scalar_one_or_none()
                    if t_rb:
                        if t_rb.current_members > 0:
                            t_rb.current_members -= 1
                        t_rb.status = "error"
                        logger.warning(f"[后台验证] Team {team_id} 虚假成功，已标记为 error（不可用）")

                    await bg_session.commit()
                except Exception as e:
                    if bg_session.in_transaction():
                        await bg_session.rollback()
                    logger.error(f"[后台验证] 虚假成功回滚失败: {e}")

            except Exception as e:
                logger.error(f"[后台验证] 后台验证任务异常: {e}")
                logger.error(traceback.format_exc())


# 创建全局实例
redeem_flow_service = RedeemFlowService()
