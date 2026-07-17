# flink-demo-parent Constitution

本憲章為 Spec-Driven Development（規格驅動開發）流程的守門規則，僅收錄「約束性原則」。
實作細節、指令與架構說明仍以專案根目錄的 `CLAUDE.md` 為準；兩者互補：
`CLAUDE.md` = 給 agent 的即時上下文，本憲章 = SDD 各產物（spec / plan / tasks）必須遵守的底線。

## Core Principles

### I. 繁體中文為專案母語（NON-NEGOTIABLE）
所有新增或修改的原始碼註解、log 訊息一律使用**繁體中文**，比照既有檔案風格。
規格與計畫文件亦以繁體中文撰寫。英文僅用於程式識別字、API 名稱與技術專有名詞。

### II. Maven 多模組結構
新增 app 模組一律放在 `app/<name>/`，其 `pom.xml` parent 為 `flink-demo-parent`。
版本、共用相依與 `maven-shade-plugin` 設定集中在 parent `pom.xml` 的 `dependencyManagement`；
子模組**只**覆寫 `maven-shade-plugin.mainClass` 屬性以指定 uber-jar 進入點，不得各自複製 shade 設定。
新模組須依賴 `flink-demo-common` 取得共用交易模型，並在 root `pom.xml` 註冊 `<module>`。

### III. 相依 scope 紀律（runtime 正確性關鍵）
- `provided`（不進 shaded jar，由容器 classpath 提供）：Flink core（`flink-streaming-java`、
  `flink-clients`）、Hadoop client、Lombok。
- `bundled`（必須打包進 shaded jar）：`flink-cep`、各 connector（Kafka、postgres-cdc）、
  `flink-statebackend-rocksdb`、Drools。
- shade 設定排除 SLF4J / Log4j，改用容器提供的 logging。
任何新增相依都必須明確歸類為以上其一，並在計畫中說明理由。

### IV. 部署經由腳本（Podman，非 Docker）
建置與提交一律走 `scripts/` 下的 bash 腳本（`job-submit.sh` 等），底層是 **Podman**。
所有服務以容器 hostname 定址（`kafka:9092`、`namenode:9000`、`postgres:5432`、`jobmanager:8081`），
不得改為 localhost 或硬編 IP。checkpoint 一律寫入 HDFS（`hdfs://namenode:9000/flink/checkpoints/...`）。

### V. 測試工具與可測性
單元/整合測試使用 **JUnit 5**；涉及 `KeyedProcessFunction`、timer、broadcast state 的邏輯
須用 Flink test harness（`flink-test-utils`、`flink-streaming-java:tests`）撰寫可重現測試。
含 timer / event-time / CDC broadcast 的變更，計畫中必須指出對應的 harness 測試策略。

## Additional Constraints（技術約束）

- Flink 1.20.1 / Java 11；不得引入需要更高 Java 版本的相依。
- flink-demo-2 的 CDC 依賴 `card_limit` 具 `REPLICA IDENTITY FULL` 且 DB 角色具 `REPLICATION`，
  使用 `pgoutput` 解碼外掛；變更 CDC 來源時須同步維護 `conf/postgres/init/01-ddl.sql`。
- Drools 決策表位於 `app/flink-demo-2/src/main/resources/rules/CardLevelRules.xlsx`，由
  `CardLevelRuleEngine` 從 classpath 載入；規則調整以此檔為單一真實來源。

## Development Workflow（SDD 流程）

1. `/speckit-constitution` — 建立/更新本憲章。
2. `/speckit-specify` — 撰寫功能規格（做什麼、為什麼，不談實作）。
3. `/speckit-clarify`（選用）— 於規劃前補齊規格中未明確處。
4. `/speckit-plan` — 產生技術實作計畫，須明確標示相依 scope（原則 III）與測試策略（原則 V）。
5. `/speckit-tasks` — 拆成可執行任務。
6. `/speckit-analyze`（選用）— 檢查跨產物一致性。
7. `/speckit-implement` — 依任務實作。
規格與計畫產物落在 `specs/<feature>/`，隨程式一併進 Git 供 PR 審閱。

## Governance

本憲章優先於臨時慣例；與本憲章衝突的規格、計畫或實作須先修正憲章或調整方案，不得逕行違反。
任何 PR / review 須確認符合以上原則；額外複雜度須在計畫中說明理由。
執行期開發指引以 `CLAUDE.md` 為準；本憲章僅規範不可違反的底線。

**Version**: 1.0.0 | **Ratified**: 2026-07-17 | **Last Amended**: 2026-07-17
