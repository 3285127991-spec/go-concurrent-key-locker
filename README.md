# keyedlocker

并发安全的 keyed locker（Go 库 + 最小 CLI 演示），无第三方依赖。

- 不同 key 的锁完全独立，可并行持有
- 同一 key 同时只有一个持有者，其余调用者按 FIFO 排队
- 成功加锁返回唯一非零 token，解锁必须凭当前持有者的 token
- 等待中 context 取消只移除自身，不影响队列中的其他 waiter
- 释放锁时所有权直接交接给该 key 的下一个有效 waiter，不插队、不双持、不丢唤醒
- Close 后拒绝新的 Lock/TryLock，唤醒所有排队 waiter 并返回 ErrClosed；已持有的 token 仍可正常 Unlock

## API

```go
lk := keyedlocker.New()
defer lk.Close()

tok, err := lk.Lock(ctx, "user:42")   // 阻塞等待，FIFO
if err != nil { /* ctx 取消或 ErrClosed */ }
err = lk.Unlock(tok)                  // 未知/重复/错误 token 返回 ErrUnknownToken

tok, err = lk.TryLock("user:42")      // 已被持有时返回 ErrLocked

snap := lk.Snapshot()                 // 按 key 排序的 []KeySnapshot{Key, Held, Token, Waiters}
n := lk.Len()                         // 当前存在锁状态的 key 数量
lk.Close()                            // 幂等；唤醒所有 waiter（ErrClosed）
```

错误均为哨兵错误，可用 errors.Is 判断：ErrLocked、ErrClosed、ErrUnknownToken。

## 运行 CLI 演示

```
go run ./cmd/keyedlocker
```

依次演示：同 key FIFO、不同 key 并行、context 取消、Close 行为。

## 测试

```
go test -race ./...
```

## 设计要点

- 单个 sync.Mutex 保护全部状态：byKey（key -> 锁状态）、byToken（token -> 锁状态）
- 每个 key 一个 FIFO waiter 队列；waiter 通过容量为 1 的 channel 接收授权
- 授权（token 或 ErrClosed）只在持锁状态下发送，因此不会丢失唤醒
- 取消与交接竞争时：若授权已送达，取消方会把所有权继续传给下一个 waiter，锁不泄漏
- key 状态在「无持有且无等待」时从 map 中删除，Len 即 map 大小

## 明确不支持

可重入锁、读写锁、超时调度器、持久化、网络、GUI、数据库。

