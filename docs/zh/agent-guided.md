# Agent 带教

当你希望在终端里由 Codex 互动带完一个 MiniRedis Stage 时使用此模式。

## 1. 在 MiniRedis 中打开 Codex

```bash
cd MiniRedis
codex
```

## 2. 直接要求开始 Stage

向 Codex 发送：

```text
开始 Agent 带教 Stage 03
```

把 `03` 换成 `01` 到 `30` 中的任意 Stage。Codex 会自动准备正确的起始状态，先用
简短问题判断当前理解，再开始讲解和实现。

## 3. 继续或稍后回来

回答 Codex 的问题、要求直接解释，或者说“继续”即可。如果中途退出，之后再次发送
同一个 Stage 请求，Codex 会自动保留并接着当前进度继续。

完成时，Codex 会运行累计测试并对照 canonical Stage 边界验收。只有确实想丢弃当前
Stage 进度时，才明确要求 Codex 重置该 Stage。

