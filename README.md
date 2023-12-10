## 本项目基于vnpy各模组版本：
vnpy: 3.6.0 -> 3.8.0
everything ok before dc60e24;

ctp_gateway: 6.6.9.0
everything ok before ae82f7b;

data_manager: 1.0.9 -> 1.1.0
everything ok before d2559b7;

dolphindb: 1.0.8;

sopt_gateway: 3.7.0.0

spread_trading: 1.2.0

tinysoft_datafeed: 2023.2.24

DataRecorder: 
base: 1.0.4; 1.0.5/1.0.6 ignored

PortfolioStrategy: 54ff2ae 1.0.7+

PaperAccount: 1.0.3 


## 已知的问题：
   - ctp 录制时，每个交易小结收盘的最后1bar 可能来自非交易时间，例如DCE 部分合约的最后1tick 可能来自15：09 等，可能造成最后1min bar 数据的不准确；也可能由于最后1tick 的遗失导致最后1bar的缺失
     > 目前采取的解决方式是不管，之后用商业数据源重刷
     > 这样还是对天软有依赖，只能说后续再想办法解决 
     > 原则：准确性、一致性
     > 

## 工作规划

考虑实际意义，重新规划路径顺序

- [x] main build / record ———— 先采用移植autobatch 的方案
- [ ] validate main quote by notebook backtest (use no night trading contracts)
- [ ] backtester
- [ ] 1 vs many
- [ ] paper trade

- [ ] rebuild all quote is a must

细节问题延后处理
问题：
由于1500 之后可能推过来tick，这些tick 会起到关闭bar的作用，但是存在两个问题：
1. 如果没有这些tick，如何合上bar
2. 这些tick 有些是有量的，需要并入最后一根bar，之前的最后一根bar可能数据不全


解决方案：
1. 对于天软冲刷，删除旧数据能很大程度解决这个问题，因为能删除多出来的bar  OK
2. 对于ctp接收，需要添加对 临近收盘时tick 的处理：缓存关闭bar、强制关闭bar —— 日中的休市会不会也有类似的问题？

- [ ] daily bar generator
- [ ] tick filter 重构 https://www.vnpy.com/forum/topic/30601-che-di-jie-jue-tickshu-ju-de-guo-lu-wen-ti?page=1
- [ ] OnRtnInstrumentStatus
- [ ] 郑商所 0.5 tick 的问题

- [ ] quote 1min transfer / download
- [ ] 0 price bar fix / BarFixer
- [ ] 20231124 start trading contract in main ticker generating


