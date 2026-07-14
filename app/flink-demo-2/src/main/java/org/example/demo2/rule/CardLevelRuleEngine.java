package org.example.demo2.rule;

import lombok.extern.slf4j.Slf4j;
import org.example.demo2.model.MonthlyConsumptionFact;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.Serializable;

@Slf4j
public class CardLevelRuleEngine implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String DECISION_TABLE_PATH = "rules/CardLevelRules.xlsx";

    private transient KieBase kieBase;

    /**
     * 依據 fact 中的 cardLimit / monthlyAmount 計算 status / description，並回填至同一個 fact 物件。
     */
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
        // 單利
        KieServices kieServices = KieServices.get();
        KieFileSystem kfs = kieServices.newKieFileSystem();
        kfs.write(
                ResourceFactory.newClassPathResource(DECISION_TABLE_PATH)
                        .setResourceType(ResourceType.DTABLE)
        );

        KieBuilder kieBuilder = kieServices.newKieBuilder(kfs);
        kieBuilder.buildAll();

        if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
            throw new IllegalStateException("載入決策表失敗: " + kieBuilder.getResults().getMessages());
        }
        KieContainer kieContainer = kieServices.newKieContainer(
                kieServices.getRepository().getDefaultReleaseId()
        );
        return kieContainer.getKieBase();
    }
}
