# keyedlocker

Go 实现的并发安全 Keyed Locker：不同 key 并行持有，同一 key 互斥且按 FIFO 排队。
仅使用标准库，无任何第三方依赖。

## 特性

- `Lock(ctx, key)`：阻塞获取锁，按 FIFO 排队；`ctx` 取消时只移除自身，不影响其他 waiter
- `TryLock(key)`：非阻塞获取，已被持有时返回 `ErrLocked`
- `Unlock(token)`：仅当前持有者的 token 有效；零值、未知或已释放的 token 返回 `ErrInvalidToken`
- `Snapshot()`：按 key 排序返回每个 key 的持有 token 与等待人数
- `Len()`：当前存在锁状态的 key 数量
- `Close()`：唤醒所有排队 waiter（返回 `ErrClosed`）并拒绝新的 Lock/TryLock；已持有的 token 仍可正常 Unlock

成功加锁返回唯一非零 `Token`。释放锁时所有权直接移交给该 key 的下一个有效 waiter，
无插队窗口，不会双持或丢失唤醒。

## 使用

```go
l := keyedlocker.New()
defer l.Close()

tok, err := l.Lock(ctx, "user-42")
if err != nil {
    return err
}
defer l.Unlock(tok)
// 临界区：同一时刻只有一个 "user-42" 持有者
```

## CLI 演示

```sh
go run ./cmd/keyedlocker
```

依次演示：同 key FIFO 顺序、不同 key 并行持有、context 取消、Close 行为。

## 测试

```sh
go test -race ./...
```

## 非目标

不实现可重入锁、读写锁、超时调度、持久化、网络、GUI 或数据库。
