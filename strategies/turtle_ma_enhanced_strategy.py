"""
================================================================================
海龟均线增强策略 (Turtle MA Enhanced Strategy)
================================================================================

策略概述：
    本策略融合了经典海龟交易法则的仓位管理体系、三线移动平均线的趋势开仓信号、
    以及基于标准差的固定止损与移动止损系统。同时引入成交量确认、波动率过滤、
    RSI 超买过滤和均线斜率过滤等多维辅助因子，专为A股做多设计。

核心模块：
    1. 开仓信号 — 三线均线多头排列 (快线 > 中线 > 慢线)
    2. 加减仓   — 海龟交易法则金字塔加仓 (每上涨 0.5×ATR 加仓, 最多4单元)
    3. 止损体系 — 固定止损 (1倍标准差) + 移动止损 (2倍标准差)
    4. 辅助因子 — 放量确认 / 波动率过滤 / RSI过滤 / 均线斜率过滤

适用市场：
    A股 (仅做多，不做空)

框架：
    VNPy CTA Strategy (vnpy_ctastrategy.CtaTemplate)

作者：TraderMate
版本：1.0
日期：2026-02-09

================================================================================
策略参数详解
================================================================================

【三线均线参数】
    fast_window   (int, 默认5)   — 快速均线窗口期
        用途：捕捉短期价格动量，最敏感的趋势指标
        调参建议：3~10，越小越敏感但假信号越多

    mid_window    (int, 默认20)  — 中速均线窗口期
        用途：确认中期趋势方向，作为趋势的主要判断线
        调参建议：15~30，对应约1个月交易日

    slow_window   (int, 默认60)  — 慢速均线窗口期
        用途：过滤大趋势方向，只在大趋势向上时开仓
        调参建议：40~120，对应约2~6个月交易日

【海龟仓位管理参数】
    atr_window    (int, 默认20)  — ATR (真实波动幅度) 计算窗口
        用途：衡量市场波动性，决定加仓间距和仓位大小
        调参建议：14~30

    fixed_size    (int, 默认1)   — 每次开仓/加仓的基础手数
        用途：控制单次下单量
        调参建议：根据账户资金和股价调整

    max_units     (int, 默认4)   — 最大加仓单元数 (含首次开仓)
        用途：限制单只股票的最大持仓比例，控制集中度风险
        调参建议：2~4，越大杠杆越高

    pyramid_step  (float, 默认0.5) — 加仓步长 (ATR的倍数)
        用途：价格每上涨 pyramid_step × ATR 时触发一次加仓
        调参建议：0.3~1.0，越小加仓越频繁

【止损参数】
    stop_loss_window      (int, 默认20)   — 计算标准差的回看窗口
        用途：决定止损价计算所用的历史波动窗口
        调参建议：10~30

    fixed_stop_multiplier (float, 默认1.0) — 固定止损标准差倍数
        用途：入场后设定不变的止损线 = 入场价 - N × 标准差
        说明：1倍标准差约覆盖68%的正常波动，较为紧密
        调参建议：0.5~2.0

    trailing_stop_multiplier (float, 默认2.0) — 移动止损标准差倍数
        用途：跟随价格上涨动态上移的止损线 = 最高价 - N × 标准差
        说明：2倍标准差约覆盖95%的正常波动，给予趋势更多空间
        调参建议：1.0~3.0

    use_stop_loss (bool, 默认True) — 是否启用止损系统
        用途：可关闭止损进行对比回测

【辅助因子参数】
    vol_confirm_window  (int, 默认20)    — 成交量均值计算窗口
        用途：计算过去N天的平均成交量

    vol_confirm_ratio   (float, 默认1.2) — 放量确认倍数
        用途：当日成交量 > 均量 × 此倍数 才确认开仓信号
        原理：放量突破更可靠，缩量突破容易回落
        调参建议：1.0~2.0

    volatility_max      (float, 默认0.06) — 最大波动率阈值 (ATR/收盘价)
        用途：波动率过高时不开仓，避免剧烈震荡期的假突破
        调参建议：0.03~0.10

    volatility_min      (float, 默认0.005) — 最小波动率阈值 (ATR/收盘价)
        用途：波动率过低时不开仓，避免横盘缩量期的无效交易
        调参建议：0.003~0.01

    rsi_window          (int, 默认14)    — RSI 计算窗口
        用途：判断超买/超卖状态

    rsi_overbought      (float, 默认70.0) — RSI超买阈值
        用途：RSI > 此值时不再加仓，控制追高风险
        调参建议：65~80

    rsi_entry_max       (float, 默认75.0) — RSI开仓上限
        用途：RSI > 此值时不开首仓，避免在极度超买区入场
        调参建议：70~85

    ma_slope_window     (int, 默认5)     — 均线斜率计算窗口
        用途：计算中期均线最近N天的斜率变化

    use_vol_confirm     (bool, 默认True)  — 是否启用成交量确认
    use_volatility_filter (bool, 默认True) — 是否启用波动率过滤
    use_rsi_filter      (bool, 默认True)  — 是否启用RSI过滤
    use_slope_filter    (bool, 默认True)  — 是否启用均线斜率过滤

================================================================================
策略逻辑流程图
================================================================================

on_bar(bar) 被调用:
│
├── 1. cancel_all()  — 撤销所有未成交挂单
├── 2. am.update_bar(bar) — 更新K线数据到ArrayManager
├── 3. if not am.inited: return — 数据预热期不操作
│
├── 4. 计算所有指标：
│      ├── 三线均线 (fast_ma, mid_ma, slow_ma)
│      ├── ATR波动率 (atr_value)
│      ├── RSI (rsi_value)
│      ├── 成交量均值 (vol_ma)
│      └── 均线斜率 (ma_slope)
│
├── 5. 判断开仓/加仓条件：
│      │
│      ├── 【无仓位时 → 判断首次开仓】
│      │   ├── ✅ 均线多头排列: fast > mid > slow
│      │   ├── ✅ 放量确认: volume > vol_ma × vol_confirm_ratio
│      │   ├── ✅ 波动率合理: volatility_min < ATR/close < volatility_max
│      │   ├── ✅ RSI未超买: RSI < rsi_entry_max
│      │   ├── ✅ 均线斜率向上: ma_slope > 0
│      │   └── → 全部满足则 buy(close, fixed_size)
│      │
│      └── 【有仓位时 → 判断加仓 or 止损 or 平仓】
│          ├── 检查止损 → 若触发则立即平仓, return
│          ├── 检查均线死叉(fast < mid) → 若触发则趋势反转平仓
│          └── 检查加仓条件:
│              ├── 当前单元数 < max_units
│              ├── 价格 > 上次入场价 + pyramid_step × ATR
│              ├── RSI < rsi_overbought
│              └── → 满足则 buy(close, fixed_size), 更新止损基准
│
└── 6. put_event() — 推送UI更新

on_trade(trade) 被调用:
│
├── 记录入场价 long_entry = trade.price
├── 更新ATR止损: long_stop = entry - 2×ATR
├── 设置标准差止损: StopLossManager.set_entry(...)
└── 更新加仓计数

================================================================================
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
import numpy as np

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    Direction,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)


# ============================================================================
# 内嵌止损模块 (来自 StopLossManager，完全自包含无外部依赖)
# ============================================================================

@dataclass
class StopLossState:
    """单个持仓的止损状态数据类。

    Attributes:
        vt_symbol:          合约代码 (如 '000001.SZSE')
        entry_price:        入场价格
        fixed_stop_price:   固定止损价格 (入场后不变)
        trailing_stop_price: 移动止损价格 (随价格上涨而上移)
        highest_price:      持仓期间的最高价 (用于移动止损计算)
        is_long:            是否为多头仓位 (A股始终为True)
    """
    vt_symbol: str
    entry_price: float
    fixed_stop_price: float
    trailing_stop_price: float
    highest_price: float
    is_long: bool = True

    def get_active_stop_price(self) -> float:
        """获取当前生效的止损价格。
        多头取固定止损与移动止损中较高者，最大限度保护利润。
        """
        if self.is_long:
            return max(self.fixed_stop_price, self.trailing_stop_price)
        return min(self.fixed_stop_price, self.trailing_stop_price)


class StopLossManager:
    """通用止损管理器 — 管理固定止损与移动止损。

    固定止损 (Fixed Stop):
        入场时一次性计算，此后不再变动。
        止损价 = 入场价 - fixed_std_multiplier × std(最近N天收盘价)

    移动止损 (Trailing Stop):
        随价格上涨动态上移，只升不降。
        止损价 = 最高价 - trailing_std_multiplier × std(最近N天收盘价)
    """

    def __init__(
        self,
        fixed_std_multiplier: float = 1.0,
        trailing_std_multiplier: float = 2.0,
        lookback_period: int = 20,
        use_fixed_stop: bool = True,
        use_trailing_stop: bool = True,
    ):
        self.fixed_std_multiplier = fixed_std_multiplier
        self.trailing_std_multiplier = trailing_std_multiplier
        self.lookback_period = lookback_period
        self.use_fixed_stop = use_fixed_stop
        self.use_trailing_stop = use_trailing_stop
        self.positions: Dict[str, StopLossState] = {}

    @staticmethod
    def _calc_std(prices: List[float]) -> float:
        """计算价格序列的样本标准差 (ddof=1)。"""
        if len(prices) < 2:
            return 0.0
        return float(np.std(prices, ddof=1))

    def set_entry(
        self, vt_symbol: str, entry_price: float,
        recent_prices: List[float], is_long: bool = True,
    ) -> StopLossState:
        """开仓时设置止损状态。"""
        std = self._calc_std(recent_prices)
        if is_long:
            fixed_stop = entry_price - self.fixed_std_multiplier * std
            trailing_stop = entry_price - self.trailing_std_multiplier * std
        else:
            fixed_stop = entry_price + self.fixed_std_multiplier * std
            trailing_stop = entry_price + self.trailing_std_multiplier * std

        state = StopLossState(
            vt_symbol=vt_symbol,
            entry_price=entry_price,
            fixed_stop_price=fixed_stop,
            trailing_stop_price=trailing_stop,
            highest_price=entry_price,
            is_long=is_long,
        )
        self.positions[vt_symbol] = state
        return state

    def update_trailing_stop(
        self, vt_symbol: str, current_price: float,
        recent_prices: List[float],
    ) -> Optional[float]:
        """每根K线调用：若价格创新高则上移移动止损 (只升不降)。"""
        if vt_symbol not in self.positions:
            return None
        state = self.positions[vt_symbol]
        std = self._calc_std(recent_prices)
        if state.is_long and current_price > state.highest_price:
            state.highest_price = current_price
            new_stop = current_price - self.trailing_std_multiplier * std
            if new_stop > state.trailing_stop_price:
                state.trailing_stop_price = new_stop
        return state.trailing_stop_price

    def should_stop_loss(self, vt_symbol: str, current_price: float) -> bool:
        """检查当前价格是否触发任一止损条件。"""
        if vt_symbol not in self.positions:
            return False
        state = self.positions[vt_symbol]
        if state.is_long:
            if self.use_fixed_stop and current_price <= state.fixed_stop_price:
                return True
            if self.use_trailing_stop and current_price <= state.trailing_stop_price:
                return True
        return False

    def get_stop_reason(self, vt_symbol: str, current_price: float) -> Optional[str]:
        """返回止损触发原因: 'fixed' / 'trailing' / None。"""
        if vt_symbol not in self.positions:
            return None
        state = self.positions[vt_symbol]
        if state.is_long:
            if self.use_fixed_stop and current_price <= state.fixed_stop_price:
                return "fixed"
            if self.use_trailing_stop and current_price <= state.trailing_stop_price:
                return "trailing"
        return None

    def get_state(self, vt_symbol: str) -> Optional[StopLossState]:
        return self.positions.get(vt_symbol)

    def remove_position(self, vt_symbol: str) -> None:
        self.positions.pop(vt_symbol, None)

    def clear_all(self) -> None:
        self.positions.clear()


# ============================================================================
# RSI 手动计算 (避免引入 talib 依赖)
# ============================================================================

def compute_rsi(closes: np.ndarray, period: int = 14) -> float:
    """使用 Wilder 平滑法计算 RSI。

    Args:
        closes: 收盘价 numpy 数组 (至少需要 period+1 个元素)
        period: RSI 周期

    Returns:
        当前 RSI 值 (0~100)，数据不足时返回 50.0 (中性值)
    """
    if len(closes) < period + 1:
        return 50.0

    deltas = np.diff(closes[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    # Wilder 指数平滑 (对剩余部分迭代)
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


# ============================================================================
# 主策略类
# ============================================================================

class TurtleMAEnhancedStrategy(CtaTemplate):
    """海龟均线增强策略 — A股专用做多策略。

    信号系统：三线均线多头排列 (fast > mid > slow)
    仓位管理：海龟法则金字塔加仓 (最多 max_units 个单元)
    止损体系：固定止损 (1×std) + 移动止损 (2×std)
    辅助因子：放量确认 / 波动率过滤 / RSI过滤 / 均线斜率过滤
    """

    author = "TraderMate"

    # ------------------------------------------------------------------
    # 策略参数 (可在UI/回测中调优)
    # ------------------------------------------------------------------

    # 三线均线
    fast_window: int = 5        # 快线窗口 (日)
    mid_window: int = 20        # 中线窗口 (日)
    slow_window: int = 60       # 慢线窗口 (日)

    # 海龟仓位管理
    atr_window: int = 20        # ATR 计算窗口
    fixed_size: int = 1         # 每次开仓/加仓手数
    max_units: int = 4          # 最大持仓单元数
    pyramid_step: float = 0.5   # 加仓步长 (ATR倍数)

    # 止损
    stop_loss_window: int = 20          # 标准差回看窗口
    fixed_stop_multiplier: float = 1.0  # 固定止损 = 入场价 - 1×std
    trailing_stop_multiplier: float = 2.0  # 移动止损 = 最高价 - 2×std
    use_stop_loss: bool = True

    # 辅助因子
    vol_confirm_window: int = 20        # 成交量均值窗口
    vol_confirm_ratio: float = 1.2      # 放量确认倍数
    volatility_max: float = 0.06        # 最大ATR/close波动率
    volatility_min: float = 0.005       # 最小ATR/close波动率
    rsi_window: int = 14                # RSI 窗口
    rsi_overbought: float = 70.0        # RSI超买阈值 (不加仓)
    rsi_entry_max: float = 75.0         # RSI开仓上限 (不首开)
    ma_slope_window: int = 5            # 均线斜率窗口

    # 辅助因子开关
    use_vol_confirm: bool = True
    use_volatility_filter: bool = True
    use_rsi_filter: bool = True
    use_slope_filter: bool = True

    # ------------------------------------------------------------------
    # 策略变量 (运行时状态，供UI显示)
    # ------------------------------------------------------------------
    fast_ma: float = 0.0
    mid_ma: float = 0.0
    slow_ma: float = 0.0
    atr_value: float = 0.0
    rsi_value: float = 50.0
    vol_ma: float = 0.0
    ma_slope: float = 0.0
    volatility: float = 0.0

    long_entry: float = 0.0        # 最近一次入场价
    long_stop: float = 0.0         # ATR止损价
    unit_count: int = 0            # 当前持仓单元数
    std_fixed_stop: float = 0.0    # 标准差固定止损价
    std_trailing_stop: float = 0.0 # 标准差移动止损价

    # ------------------------------------------------------------------
    # vnpy 序列化声明
    # ------------------------------------------------------------------
    parameters = [
        # 均线
        "fast_window", "mid_window", "slow_window",
        # 海龟
        "atr_window", "fixed_size", "max_units", "pyramid_step",
        # 止损
        "stop_loss_window", "fixed_stop_multiplier",
        "trailing_stop_multiplier", "use_stop_loss",
        # 辅助因子
        "vol_confirm_window", "vol_confirm_ratio",
        "volatility_max", "volatility_min",
        "rsi_window", "rsi_overbought", "rsi_entry_max",
        "ma_slope_window",
        # 开关
        "use_vol_confirm", "use_volatility_filter",
        "use_rsi_filter", "use_slope_filter",
    ]

    variables = [
        "fast_ma", "mid_ma", "slow_ma",
        "atr_value", "rsi_value", "vol_ma",
        "ma_slope", "volatility",
        "long_entry", "long_stop", "unit_count",
        "std_fixed_stop", "std_trailing_stop",
    ]

    # ==================================================================
    # 生命周期方法
    # ==================================================================

    def on_init(self) -> None:
        """策略初始化。

        创建 BarGenerator 与 ArrayManager，初始化止损管理器，
        加载足够的历史K线使指标预热完成 (am.inited == True)。
        """
        self.write_log("海龟均线增强策略 初始化")

        self.bg: BarGenerator = BarGenerator(self.on_bar)
        self.am: ArrayManager = ArrayManager(size=150)  # 确保容纳慢线+RSI预热

        # 初始化止损管理器 (参数由策略参数传入)
        self.stop_loss_mgr = StopLossManager(
            fixed_std_multiplier=self.fixed_stop_multiplier,
            trailing_std_multiplier=self.trailing_stop_multiplier,
            lookback_period=self.stop_loss_window,
            use_fixed_stop=self.use_stop_loss,
            use_trailing_stop=self.use_stop_loss,
        )

        # 加仓计数器
        self.unit_count = 0

        # 加载历史K线：需要 slow_window + rsi_window + 余量
        bars_needed = max(self.slow_window, self.stop_loss_window) + self.rsi_window + 20
        self.load_bar(bars_needed)

    def on_start(self) -> None:
        """策略启动。"""
        self.write_log("海龟均线增强策略 启动")

    def on_stop(self) -> None:
        """策略停止。"""
        self.write_log("海龟均线增强策略 停止")

    def on_tick(self, tick: TickData) -> None:
        """Tick数据更新 → 合成K线。"""
        self.bg.update_tick(tick)

    # ==================================================================
    # 核心K线逻辑
    # ==================================================================

    def on_bar(self, bar: BarData) -> None:
        """每根K线触发的核心决策逻辑。

        执行流程:
            1. 撤销所有未成交挂单
            2. 更新K线到ArrayManager
            3. 等待预热完成
            4. 计算全部技术指标
            5. 根据持仓状态进入 开仓/加仓/止损/平仓 分支
            6. 推送UI事件
        """
        # ---- Step 1: 清除旧的未成交订单 ----
        self.cancel_all()

        # ---- Step 2: 更新K线数据 ----
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # ---- Step 3: 计算技术指标 ----
        self._calculate_indicators(bar)

        # ---- Step 4: 构造合约代码 (用于止损管理器) ----
        vt_symbol = f"{bar.symbol}.{bar.exchange.value}"

        # ---- Step 5: 决策逻辑分支 ----
        if self.pos == 0:
            # === 空仓 → 寻找开仓机会 ===
            self._handle_no_position(bar, vt_symbol)
        elif self.pos > 0:
            # === 持仓 → 止损 / 平仓 / 加仓 ===
            self._handle_long_position(bar, vt_symbol)

        # ---- Step 6: 推送UI更新 ----
        self.put_event()

    # ==================================================================
    # 指标计算
    # ==================================================================

    def _calculate_indicators(self, bar: BarData) -> None:
        """计算当前K线所有技术指标并更新策略变量。"""

        # --- 三线均线 ---
        self.fast_ma = self.am.sma(self.fast_window)
        self.mid_ma = self.am.sma(self.mid_window)
        self.slow_ma = self.am.sma(self.slow_window)

        # --- ATR (平均真实波动幅度) ---
        self.atr_value = self.am.atr(self.atr_window)

        # --- 波动率 = ATR / 收盘价 ---
        self.volatility = self.atr_value / bar.close_price if bar.close_price > 0 else 0.0

        # --- RSI ---
        self.rsi_value = compute_rsi(self.am.close, self.rsi_window)

        # --- 成交量均值 ---
        if len(self.am.volume) >= self.vol_confirm_window:
            self.vol_ma = float(np.mean(self.am.volume[-self.vol_confirm_window:]))
        else:
            self.vol_ma = 0.0

        # --- 中期均线斜率 ---
        # 斜率 = (当前mid_ma - N天前mid_ma) / N天前mid_ma
        if len(self.am.close) >= self.mid_window + self.ma_slope_window:
            mid_ma_prev = float(np.mean(
                self.am.close[-(self.mid_window + self.ma_slope_window):-(self.ma_slope_window)]
            ))
            self.ma_slope = (self.mid_ma - mid_ma_prev) / mid_ma_prev if mid_ma_prev > 0 else 0.0
        else:
            self.ma_slope = 0.0

    # ==================================================================
    # 空仓处理 — 首次开仓判断
    # ==================================================================

    def _handle_no_position(self, bar: BarData, vt_symbol: str) -> None:
        """空仓状态下判断是否满足开仓条件。

        开仓需同时满足:
            1. 均线多头排列: fast_ma > mid_ma > slow_ma
            2. 放量确认 (可选): 当日成交量 > 均量 × vol_confirm_ratio
            3. 波动率合理 (可选): volatility_min < ATR/close < volatility_max
            4. RSI未极度超买 (可选): RSI < rsi_entry_max
            5. 均线斜率向上 (可选): ma_slope > 0
        """
        # 重置止损状态
        self.stop_loss_mgr.remove_position(vt_symbol)
        self.long_entry = 0.0
        self.long_stop = 0.0
        self.unit_count = 0
        self.std_fixed_stop = 0.0
        self.std_trailing_stop = 0.0

        # --- 条件1: 均线多头排列 (必须) ---
        if not (self.fast_ma > self.mid_ma > self.slow_ma):
            return

        # --- 条件2: 放量确认 ---
        if self.use_vol_confirm and self.vol_ma > 0:
            if bar.volume < self.vol_ma * self.vol_confirm_ratio:
                return

        # --- 条件3: 波动率过滤 ---
        if self.use_volatility_filter:
            if self.volatility > self.volatility_max or self.volatility < self.volatility_min:
                return

        # --- 条件4: RSI过滤 ---
        if self.use_rsi_filter:
            if self.rsi_value > self.rsi_entry_max:
                return

        # --- 条件5: 均线斜率过滤 ---
        if self.use_slope_filter:
            if self.ma_slope <= 0:
                return

        # === 全部条件满足 → 开仓 ===
        self.write_log(
            f"开仓信号: MA排列={self.fast_ma:.2f}>{self.mid_ma:.2f}>{self.slow_ma:.2f}, "
            f"RSI={self.rsi_value:.1f}, Vol比={bar.volume / self.vol_ma:.2f}"
        )
        self.buy(bar.close_price * 1.01, self.fixed_size)  # 稍高于收盘价确保成交

    # ==================================================================
    # 持仓处理 — 止损 / 趋势反转平仓 / 加仓
    # ==================================================================

    def _handle_long_position(self, bar: BarData, vt_symbol: str) -> None:
        """持仓状态下的处理逻辑。

        优先级:
            1. 止损检查 (最高优先级)
            2. 趋势反转平仓 (快线 < 中线)
            3. 海龟金字塔加仓
        """
        recent_closes = list(self.am.close[-self.stop_loss_window:])

        # ---- 1. 更新并检查止损 ----
        if self.use_stop_loss:
            self.stop_loss_mgr.update_trailing_stop(vt_symbol, bar.close_price, recent_closes)
            state = self.stop_loss_mgr.get_state(vt_symbol)

            if state:
                self.std_fixed_stop = state.fixed_stop_price
                self.std_trailing_stop = state.trailing_stop_price

                if self.stop_loss_mgr.should_stop_loss(vt_symbol, bar.close_price):
                    reason = self.stop_loss_mgr.get_stop_reason(vt_symbol, bar.close_price)
                    stop_price = state.get_active_stop_price()
                    self.write_log(
                        f"触发{reason}止损: 当前价={bar.close_price:.2f}, "
                        f"固定止损={state.fixed_stop_price:.2f}, "
                        f"移动止损={state.trailing_stop_price:.2f}"
                    )
                    self.sell(bar.close_price * 0.99, abs(self.pos))
                    self._reset_position_state(vt_symbol)
                    return

        # ---- 2. 趋势反转平仓: 快线下穿中线 ----
        if self.fast_ma < self.mid_ma:
            self.write_log(
                f"均线死叉平仓: fast_ma={self.fast_ma:.2f} < mid_ma={self.mid_ma:.2f}"
            )
            self.sell(bar.close_price * 0.99, abs(self.pos))
            self._reset_position_state(vt_symbol)
            return

        # ---- 3. 海龟金字塔加仓 ----
        if self.unit_count < self.max_units and self.long_entry > 0 and self.atr_value > 0:
            # 加仓条件: 价格 > 上次入场 + step × ATR
            add_threshold = self.long_entry + self.pyramid_step * self.atr_value

            if bar.close_price > add_threshold:
                # RSI 过滤: 超买区不加仓
                if self.use_rsi_filter and self.rsi_value > self.rsi_overbought:
                    self.write_log(
                        f"RSI超买({self.rsi_value:.1f}>{self.rsi_overbought}), 跳过加仓"
                    )
                else:
                    self.write_log(
                        f"海龟加仓: 第{self.unit_count + 1}单元, "
                        f"价格={bar.close_price:.2f} > 阈值={add_threshold:.2f}"
                    )
                    self.buy(bar.close_price * 1.01, self.fixed_size)

    # ==================================================================
    # 成交回报
    # ==================================================================

    def on_trade(self, trade: TradeData) -> None:
        """成交回报处理。

        做多成交时:
            1. 更新入场价
            2. 设置ATR止损线 (入场价 - 2×ATR)
            3. 设置/更新标准差止损 (StopLossManager)
            4. 更新加仓计数
        """
        if trade.direction != Direction.LONG:
            return  # A股仅做多

        vt_symbol = f"{trade.symbol}.{trade.exchange.value}"
        recent_closes = list(self.am.close[-self.stop_loss_window:])

        # 更新入场价和ATR止损
        self.long_entry = trade.price
        self.long_stop = trade.price - 2.0 * self.atr_value

        # 更新加仓计数
        self.unit_count = int(self.pos / self.fixed_size) if self.fixed_size > 0 else 0

        # 设置标准差止损
        if self.use_stop_loss and len(recent_closes) >= 2:
            state = self.stop_loss_mgr.set_entry(
                vt_symbol, trade.price, recent_closes, is_long=True
            )
            self.std_fixed_stop = state.fixed_stop_price
            self.std_trailing_stop = state.trailing_stop_price
            self.write_log(
                f"成交: 价={trade.price:.2f}, "
                f"单元={self.unit_count}/{self.max_units}, "
                f"固定止损={self.std_fixed_stop:.2f}, "
                f"移动止损={self.std_trailing_stop:.2f}, "
                f"ATR止损={self.long_stop:.2f}"
            )

    def on_order(self, order: OrderData) -> None:
        """委托回报 — 本策略无额外处理。"""
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        """停止单回报 — 本策略无额外处理。"""
        pass

    # ==================================================================
    # 内部辅助方法
    # ==================================================================

    def _reset_position_state(self, vt_symbol: str) -> None:
        """平仓后重置所有仓位相关状态。"""
        self.long_entry = 0.0
        self.long_stop = 0.0
        self.unit_count = 0
        self.std_fixed_stop = 0.0
        self.std_trailing_stop = 0.0
        self.stop_loss_mgr.remove_position(vt_symbol)