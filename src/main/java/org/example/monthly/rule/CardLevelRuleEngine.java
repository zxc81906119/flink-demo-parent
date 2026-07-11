package org.example.monthly.rule;

import lombok.extern.slf4j.Slf4j;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.Serializable;

/**
 * Drools 規則引擎封裝，載入 Excel 決策表（rules/CardLevelRules.xlsx）判斷信用卡分級。
 *
 * 決策表規則（依額度上限 cardLimit 與當月消費金額 monthlyAmount 判斷）：
 * <pre>
 * cardLimit <= 100000                                            -> status=1 建議提升上限
 * 100000 < cardLimit <= 500000 且 monthlyAmount >= cardLimit-5000 -> status=2 自動提高上限
 * 100000 < cardLimit <= 500000 且 monthlyAmount <  cardLimit-5000 -> status=3 需促銷
 * 500000 < cardLimit <= 1000000 且 monthlyAmount >= cardLimit-5000 -> status=4 可換發黑卡
 * 500000 < cardLimit <= 1000000 且 monthlyAmount <  cardLimit-5000 -> status=5 不需調整
 * cardLimit > 1000000                                            -> status=5 不需調整
 * </pre>
 *
 * 每次呼叫 {@link #evaluate(MonthlyConsumptionFact)} 皆建立全新的 KieSession，
 * 評估完成即 dispose，避免長時間持有 session 造成 fact 累積或跨執行緒共用問題。
 * {@link KieBase} 本身無狀態、執行緒安全，僅需建置一次並重複使用。
 */
@Slf4j
public class CardLevelRuleEngine implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String DECISION_TABLE_PATH = "rules/CardLevelRules.xlsx";

    private transient KieBase kieBase;

    /** 依據 fact 中的 cardLimit / monthlyAmount 計算 status / description，並回填至同一個 fact 物件。 */
    public MonthlyConsumptionFact evaluate(MonthlyConsumptionFact fact) {
        KieSession kieSession = getKieBase().newKieSession();
        try {
            kieSession.insert(fact);
            int firedCount = kieSession.fireAllRules();
            log.info("規則引擎執行完畢: cardNumber={}, yearMonth={}, firedRules={}, status={}, description={}",
                    fact.getCardNumber(), fact.getYearMonth(), firedCount, fact.getStatus(), fact.getDescription());
            return fact;
        } finally {
            kieSession.dispose();
        }
    }

    private KieBase getKieBase() {
        if (kieBase == null) {
            kieBase = buildKieBase();
        }
        return kieBase;
    }

    private KieBase buildKieBase() {
        KieServices kieServices = KieServices.Factory.get();
        KieFileSystem kfs = kieServices.newKieFileSystem();
        kfs.write(ResourceFactory.newClassPathResource(DECISION_TABLE_PATH)
                .setResourceType(ResourceType.DTABLE));

        KieBuilder kieBuilder = kieServices.newKieBuilder(kfs);
        kieBuilder.buildAll();

        if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
            throw new IllegalStateException("載入決策表失敗: " + kieBuilder.getResults().getMessages());
        }

        return kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId())
                .getKieBase();
    }
}
