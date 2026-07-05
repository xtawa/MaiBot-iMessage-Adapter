---
name: use-official-sdk-methods
description: 优先使用官方 SDK 公开 API，禁止未经批准访问内部实现
metadata:
  type: feedback
---

所有代码必须优先使用官方 SDK 的公开方法和 API。只有当官方 SDK 明确没有提供某个功能时，才可以在获得用户批准后自己实现替代方案。严禁在未经批准的情况下访问 SDK 的内部属性（如 `__internal`、`platforms` 等）或绕过 SDK 的类型系统。

**Why:** 之前 iMessage Adapter 的冷发送功能中，开发者未经批准直接访问 `app.__internal.platforms.get("iMessage")` 绕过 SDK，导致 `ContentBuilder` 未 `.build()` 的 bug。后来发现升级 SDK 到 8.x 后，`im.space.get(id)` 公开 API 就能解决问题。

**How to apply:**
1. 遇到问题先查官方文档（如 https://docs.photon.codes/docs/llms.txt）
2. 检查当前 SDK 版本是否过旧，优先升级而非绕路
3. 如果确认官方 SDK 没有对应 API，必须先向用户说明情况并获得批准
4. 所有导入只用 `spectrum-ts` 的公开导出，不要 `import` 内部 chunk
[[imessage-adapter-sidecar]]
