package org.example.demo2.rule;

import org.example.demo2.model.MonthlyConsumptionFact;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 驗證 CardLevelRules.xlsx 決策表所有分級分支皆可正確命中。
 */
class CardLevelRuleEngineTest {

    private final CardLevelRuleEngine ruleEngine = new CardLevelRuleEngine();

    static Stream<Arguments> rules() {
        return Stream.of(
                // cardLimit, monthlyAmount, expectedStatus, expectedDescription
                Arguments.of(80000.0, 50000.0, 1, "建議提升上限"),
                Arguments.of(300000.0, 296000.0, 2, "自動提高上限"),
                Arguments.of(300000.0, 100000.0, 3, "需促銷"),
                Arguments.of(800000.0, 796000.0, 4, "可換發黑卡"),
                Arguments.of(800000.0, 100000.0, 5, "不需調整"),
                Arguments.of(1200000.0, 100000.0, 5, "不需調整")
        );
    }

    @ParameterizedTest
    @MethodSource("rules")
    void evaluate_shouldReturnExpectedStatusAndDescription(
            double cardLimit, double monthlyAmount, int expectedStatus, String expectedDescription) {
        MonthlyConsumptionFact fact = new MonthlyConsumptionFact();
        fact.setCardNumber("TEST_CARD");
        fact.setYearMonth("202601");
        fact.setCardLimit(cardLimit);
        fact.setMonthlyAmount(monthlyAmount);

        MonthlyConsumptionFact result = ruleEngine.evaluate(fact);

        assertEquals(expectedStatus, result.getStatus());
        assertEquals(expectedDescription, result.getDescription());
    }

    @Test
    void evaluate_boundaryAt100000_shouldBeStatus1() {
        MonthlyConsumptionFact fact = new MonthlyConsumptionFact();
        fact.setCardLimit(100000.0);
        fact.setMonthlyAmount(1000.0);

        MonthlyConsumptionFact result = ruleEngine.evaluate(fact);

        assertEquals(1, result.getStatus());
    }
}
