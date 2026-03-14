"""
TaskHistoryStore — 基于 Redis Hash + SortedSet 的轻量任务历史层

数据结构：
  HASH  qtask:hist:{queue}:{task_id}  — 任务详情字段
  ZSET  qtask:hist_idx:{queue}        — score=created_ts，value=task_id（用于范围查询和过期清理）
  HASH  qtask:settings                — 全局设置（如 history_keep_days）

任务状态枚举：
  pending   — 已入队尚未 ACK
  completed — 已成功 ACK
  failed    — 已移入 DLQ
"""
import time
import json
from typing import Optional, List, Dict, Any

DEFAULT_KEEP_DAYS = 15
SETTINGS_KEY = "qtask:settings"


class TaskHistoryStore:
    """轻量级任务历史记录，依托 Redis，不引入额外数据库"""

    def __init__(self, redis_client, queue_name: str):
        """
        :param redis_client: 已初始化的 redis 客户端（decode_responses=True）
        :param queue_name:   队列基础名称（不含 :stream 后缀），如 "spider:tasks"
        """
        self.r = redis_client
        self.queue_name = queue_name
        self._hist_prefix = f"qtask:hist:{queue_name}"
        self._idx_key = f"qtask:hist_idx:{queue_name}"

    # ─────────────────────── 写入接口 ───────────────────────

    def record_push(self, task_id: str, action: str, payload_preview: str = ""):
        """任务入队时调用，状态设为 pending"""
        now = time.time()
        key = f"{self._hist_prefix}:{task_id}"
        self.r.hset(key, mapping={
            "task_id":        task_id,
            "queue":          self.queue_name,
            "action":         action,
            "status":         "pending",
            "retries":        0,
            "payload_preview": payload_preview[:200],  # 只保留截断预览
            "created_at":     now,
            "updated_at":     now,
            "duration_s":     "",
            "fail_reason":    "",
        })
        # 加入时间索引
        self.r.zadd(self._idx_key, {task_id: now})
        # 设置 Hash 的 TTL（以 keep_days 为准）以防孤儿 key
        self._set_hash_ttl(key)

    def record_ack(self, task_id: str, duration_s: float = 0.0):
        """任务成功 ACK 时调用"""
        now = time.time()
        key = f"{self._hist_prefix}:{task_id}"
        self.r.hset(key, mapping={
            "status":     "completed",
            "updated_at": now,
            "duration_s": round(duration_s, 3),
        })
        self._ensure_indexed(task_id, now)

    def record_fail(self, task_id: str, reason: str = ""):
        """任务移入 DLQ 时调用"""
        now = time.time()
        key = f"{self._hist_prefix}:{task_id}"
        self.r.hset(key, mapping={
            "status":      "failed",
            "updated_at":  now,
            "fail_reason": reason[:500],
        })
        self._ensure_indexed(task_id, now)

    def record_retry(self, task_id: str):
        """每次重试时调用，重试次数 +1"""
        key = f"{self._hist_prefix}:{task_id}"
        self.r.hincrby(key, "retries", 1)
        self.r.hset(key, "updated_at", time.time())

    # ─────────────────────── 查询接口 ───────────────────────

    def get_tasks(
        self,
        status: str = "all",
        limit: int = 100,
        days: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        查询历史任务列表。

        :param status: "all" / "pending" / "completed" / "failed"
        :param limit:  最多返回条数
        :param days:   只查最近 N 天，None 则用全局 keep_days 设置
        :return:       任务信息 list（按 created_at 降序）
        """
        keep_days = days if days is not None else self.get_keep_days()
        min_ts = time.time() - keep_days * 86400

        # 从 SortedSet 按时间倒序取出 task_id 列表
        candidate_ids = self.r.zrevrangebyscore(
            self._idx_key, "+inf", min_ts, start=0, num=limit * 3  # 多取一些供过滤
        )

        results = []
        for tid in candidate_ids:
            if len(results) >= limit:
                break
            key = f"{self._hist_prefix}:{tid}"
            data = self.r.hgetall(key)
            if not data:
                continue
            if status != "all" and data.get("status") != status:
                continue
            # 类型转换
            data["retries"] = int(data.get("retries", 0))
            data["created_at"] = float(data.get("created_at", 0))
            data["updated_at"] = float(data.get("updated_at", 0))
            data["duration_s"] = float(data["duration_s"]) if data.get("duration_s") else None
            results.append(data)

        return results

    def count_by_status(self) -> Dict[str, int]:
        """统计各状态任务数量（最近 keep_days 天）"""
        keep_days = self.get_keep_days()
        min_ts = time.time() - keep_days * 86400
        candidate_ids = self.r.zrangebyscore(self._idx_key, min_ts, "+inf")

        counts = {"pending": 0, "completed": 0, "failed": 0, "total": 0}
        for tid in candidate_ids:
            key = f"{self._hist_prefix}:{tid}"
            st = self.r.hget(key, "status")
            if st and st in counts:
                counts[st] += 1
            counts["total"] += 1
        return counts

    # ─────────────────────── 清理接口 ───────────────────────

    def cleanup_old(self, keep_days: Optional[int] = None):
        """删除超过 keep_days 天的历史记录（在写操作时懒触发）"""
        if keep_days is None:
            keep_days = self.get_keep_days()
        cutoff = time.time() - keep_days * 86400
        # 移除索引中过期的 task_id
        old_ids = self.r.zrangebyscore(self._idx_key, "-inf", cutoff)
        if old_ids:
            pipe = self.r.pipeline()
            for tid in old_ids:
                pipe.delete(f"{self._hist_prefix}:{tid}")
            pipe.zremrangebyscore(self._idx_key, "-inf", cutoff)
            pipe.execute()
        return len(old_ids)

    # ─────────────────────── 设置接口 ───────────────────────

    def get_keep_days(self) -> int:
        """读取全局 history_keep_days 设置"""
        try:
            val = self.r.hget(SETTINGS_KEY, "history_keep_days")
            return int(val) if val else DEFAULT_KEEP_DAYS
        except Exception:
            return DEFAULT_KEEP_DAYS

    @staticmethod
    def get_keep_days_from_redis(redis_client) -> int:
        """静态方法：不需要实例化就能读取全局设置"""
        try:
            val = redis_client.hget(SETTINGS_KEY, "history_keep_days")
            return int(val) if val else DEFAULT_KEEP_DAYS
        except Exception:
            return DEFAULT_KEEP_DAYS

    @staticmethod
    def set_keep_days(redis_client, keep_days: int):
        """全局写入 keep_days 设置"""
        redis_client.hset(SETTINGS_KEY, "history_keep_days", keep_days)

    # ─────────────────────── 内部工具 ───────────────────────

    def _set_hash_ttl(self, key: str):
        """根据 keep_days 设置 Hash 的 Redis TTL，防止孤儿 key"""
        try:
            keep_days = self.get_keep_days()
            # 比 keep_days 多一天的 buffer
            self.r.expire(key, (keep_days + 1) * 86400)
        except Exception:
            pass

    def _ensure_indexed(self, task_id: str, ts: float):
        """
        确保 task_id 在 SortedSet 索引中存在。
        解决场景：
          - 生产者和消费者使用不同 SmartQueue 实例（不同 namespace 配置）
          - 索引被手动清除后，Worker 继续消费并 ack
          - record_push 调用在生产者侧，record_ack 在消费者侧
        用 NX 语义（只在不存在时设置）避免覆盖 record_push 写入的更精确时间
        """
        try:
            # zadd NX: only add if member doesn't already exist
            self.r.zadd(self._idx_key, {task_id: ts}, nx=True)
        except Exception:
            pass

    def get_queue_summary(self) -> Dict[str, Any]:
        """返回该队列的历史摘要，用于 overview 展示"""
        counts = self.count_by_status()
        return {
            "queue":     self.queue_name,
            "counts":    counts,
            "keep_days": self.get_keep_days(),
        }
