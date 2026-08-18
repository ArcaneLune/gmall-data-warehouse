# 阶段十一：ADS 层与数据可视化

## 1. Sugar 可视化大屏端部署

Sugar 是百度云提供的 BI 可视化平台，负责最终的大屏渲染展示，部署流程如下：

1. **入口与账号**：访问官方地址 `https://cloud.baidu.com/product/sugar.html`，登录百度账号。
2. **创建组织**：新建组织，产品版本选择「大屏尝鲜版」（首次使用提供 1 个月试用期），填写组织名称完成创建。
3. **进入工作空间**：组织创建完成后，选择「进入组织」，进入系统默认生成的「第一个空间」。
4. **新建大屏**：在空间内找到「待创建大屏」，点击「新建」；可选择空白模板或预置业务模板，指定大屏名称后即可进入编辑页面。

## 2. SpringBoot 数据接口服务部署

该模块（`gmall-publisher-2026`）是数据查询的核心服务，负责对接 Doris 并向 Sugar 提供标准化 HTTP 接口。

### 2.1 模块基础信息

在 `gmall-realtime-2026` 父项目下新建 Maven 模块：

- 包路径：`com.flink.gmall.publisher`
- JDK 版本：1.8
- Spring Boot 版本：2.6.6

依赖先不添加，后续统一在 pom.xml 中添加。

> 具体项目结构和代码见项目 gmall-publisher-2026。

## 3. 内网穿透部署（钉钉穿透工具）

### 3.1 作用

将本地 8070 端口的 SpringBoot 服务映射为公网可访问域名，让部署在公网的 Sugar 大屏能够调用本地接口。

### 3.2 部署步骤

1. **下载工具**：在已安装 Git 的环境中执行克隆命令：

   ```
   git clone https://github.com/open-dingtalk/dingtalk-pierced-client.git
   ```

2. **进入执行目录**：Windows 系统进入 clone 的目录，内含固定配置文件 `ding.cfg` 和执行程序 `ding.exe`。

3. **启动穿透服务**：在目录下打开 cmd，执行启动命令：

   ```
   ding --config ding.cfg --subdomain dinghhh 8070
   ```

   - `--config ding.cfg`：钉钉官方固定配置，无需修改
   - `--subdomain dinghhh`：自定义公网域名前缀，最终映射域名为 `dinghhh.vaiwan.cn`
   - `8070`：本地 SpringBoot 服务的端口

### 3.3 映射效果

启动完成后，所有公网对 `http://dinghhh.vaiwan.cn/xxx` 的请求，都会自动转发到本地 `http://localhost:8070/xxx`。

## 4. ADS 层搭建与可视化

### 4.1 流量主题

#### 4.1.1 各渠道流量统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 渠道 | 独立访客数 | 统计访问人数 |
| 当日 | 渠道 | 会话总数 | 统计会话总数 |
| 当日 | 渠道 | 会话平均浏览页面数 | 统计每个会话平均浏览页面数 |
| 当日 | 渠道 | 会话平均停留时长 | 统计每个会话平均停留时长 |
| 当日 | 渠道 | 跳出率 | 只有一个页面的会话的比例 |

> 具体代码见项目本身。

2）Sugar 配置

（1）页面宏定义变量：单击大屏空白处，在右侧"控制面板"中点击"页面宏定义变量"，定义 GMALL_HOST 和 GMALL_DATE。

- GMALL_HOST：本地 8070 端口服务映射的公网域名
- GMALL_DATE：当日日期（本项目的"当日"为模拟数据的业务日期）

（2）在"图表"中选择"柱状图"。

（3）在弹出的控制面板中选择数据绑定方式：

- **API 拉取**：通过给定的数据接口获取数据（本项目选择这种方式）
- **静态 JSON**：通过给定的静态 JSON 字符串获取数据

API 拉取返回的数据格式与静态 JSON 相同，因此可以通过静态 JSON 的示例数据查看数据格式。

（4）输入数据接口的 URL，刷新图表查看效果。另外四张柱状图的配置同理，所有图表的 URL 如下：

```
# 独立访客数
${GMALL_HOST}/gmall/realtime/traffic/uvCt?date=${GMALL_DATE}

# 会话数
${GMALL_HOST}/gmall/realtime/traffic/svCt?date=${GMALL_DATE}

# 会话平均页面浏览数
${GMALL_HOST}/gmall/realtime/traffic/pvPerSession?date=${GMALL_DATE}

# 会话平均访问时长
${GMALL_HOST}/gmall/realtime/traffic/durPerSession?date=${GMALL_DATE}

# 跳出率
${GMALL_HOST}/gmall/realtime/traffic/ujRate?date=${GMALL_DATE}
```

#### 4.1.2 流量分时统计

1）需求说明

| 统计周期 | 指标 | 说明 |
| --- | --- | --- |
| 1 小时 | 独立访客数 | 统计当日各小时独立访客数 |
| 1 小时 | 页面浏览数 | 统计当日各小时页面浏览数 |
| 1 小时 | 新访客数 | 统计当日各小时新访客数 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择折线图。

（2）数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/traffic/visitorPerHr?date=${GMALL_DATE}
```

（3）刷新图表，查看效果。

#### 4.1.3 新老访客流量统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 访客类型 | 访客数 | 分别统计新老访客数 |
| 当日 | 访客类型 | 页面浏览数 | 分别统计新老访客页面浏览数 |
| 当日 | 访客类型 | 跳出率 | 分别统计新老访客跳出率 |
| 当日 | 访客类型 | 平均在线时长 | 分别统计新老访客平均在线时长 |
| 当日 | 访客类型 | 平均访问页面数 | 分别统计新老访客平均访问页面数 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择表格。

（2）数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/traffic/visitorPerType?date=${GMALL_DATE}
```

#### 4.1.4 关键词统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 关键词 | 关键词评分 | 根据不同来源和频次计算得分 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择 3D 词云。

（2）数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/traffic/keywords?date=${GMALL_DATE}
```

### 4.2 用户主题

#### 4.2.1 用户变动统计

1）需求说明

| 统计周期 | 指标 | 说明 |
| --- | --- | --- |
| 当日 | 回流用户数 | 之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。此处要求统计回流用户总数。 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/user/userChangeCt?date=${GMALL_DATE}
```

#### 4.2.2 用户新增活跃统计

1）需求说明

| 统计周期 | 指标 | 指标说明 |
| --- | --- | --- |
| 当日 | 新增用户数 | 略 |
| 当日 | 活跃用户数 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/user/userChangeCt?date=${GMALL_DATE}
```

#### 4.2.3 用户行为漏斗分析

1）需求说明

该需求要求统计一个完整的购物流程各个阶段的人数，具体说明如下：

| 统计周期 | 指标 | 说明 |
| --- | --- | --- |
| 当日 | 首页浏览人数 | 略 |
| 当日 | 商品详情页浏览人数 | 略 |
| 当日 | 加购人数 | 略 |
| 当日 | 下单人数 | 略 |
| 当日 | 支付人数 | 支付成功人数 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格。

（2）数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/user/uvPerPage?date=${GMALL_DATE}
```

#### 4.2.4 新增交易用户统计

1）需求说明

| 统计周期 | 指标 | 说明 |
| --- | --- | --- |
| 当日 | 新增下单人数 | 略 |
| 当日 | 新增支付人数 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/user/userTradeCt?date=${GMALL_DATE}
```

### 4.3 商品主题

#### 4.3.1 各品牌商品交易统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 品牌 | 订单金额 | 略 |
| 当日 | 品牌 | 退单数 | 略 |
| 当日 | 品牌 | 退单人数 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/commodity/trademark?date=${GMALL_DATE}
```

#### 4.3.2 各品类商品交易统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 品类 | 订单金额 | 略 |
| 当日 | 品类 | 退单数 | 略 |
| 当日 | 品类 | 退单人数 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/commodity/category?date=${GMALL_DATE}
```

#### 4.3.3 各 SPU 商品交易统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | SPU | 订单金额 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/commodity/spu?date=${GMALL_DATE}
```

### 4.4 交易主题

#### 4.4.1 交易综合统计

1）需求说明

| 统计周期 | 指标 | 说明 |
| --- | --- | --- |
| 当日 | 订单总额 | 订单最终金额 |
| 当日 | 订单数 | 略 |
| 当日 | 订单人数 | 略 |
| 当日 | 退单数 | 略 |
| 当日 | 退单人数 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择数字翻牌器。

（2）数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/trade/total?date=${GMALL_DATE}
```

（3）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/trade/stats?date=${GMALL_DATE}
```

#### 4.4.2 各省份交易统计

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 省份 | 订单数 | 略 |
| 当日 | 省份 | 订单金额 | 略 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择中国省份色彩地图。

（2）数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
# 订单数 URL
${GMALL_HOST}/gmall/realtime/trade/provinceOrderCt?date=${GMALL_DATE}
# 订单金额 URL
${GMALL_HOST}/gmall/realtime/trade/provinceOrderAmount?date=${GMALL_DATE}
```

### 4.5 优惠券主题

#### 4.5.1 当日优惠券补贴率

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 优惠券 | 补贴率 | 用券的订单明细优惠券减免金额总和/原始金额总和 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/coupon/stats?date=${GMALL_DATE}
```

### 4.6 活动主题

#### 4.6.1 当日活动补贴率

1）需求说明

| 统计周期 | 统计粒度 | 指标 | 说明 |
| --- | --- | --- | --- |
| 当日 | 活动 | 补贴率 | 参与促销活动的订单明细活动减免金额总和/原始金额总和 |

> 具体代码见项目本身。

2）Sugar 配置

（1）选择轮播表格，数据绑定方式为"API 拉取"，输入数据接口的 URL，如下：

```
${GMALL_HOST}/gmall/realtime/activity/stats?date=${GMALL_DATE}
```
