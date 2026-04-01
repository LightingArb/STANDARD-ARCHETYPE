# 幣安API文件摘要

- 行情資料：可透過 WebSocket 訂閱 ticker、depth、user data stream。
- 下單資料：REST 端點負責現貨與合約下單，需注意 timestamp 與 recvWindow。
- 風險提醒：若 API key 有交易權限，demo 階段必須預設停用 live mode。
