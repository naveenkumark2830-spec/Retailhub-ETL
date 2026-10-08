from score_calculator import calculate_risk
from response_engine import decide_response


def test_high():

    assessment = calculate_risk(
        fraud_type="BRUTE_FORCE_LOGIN",
        severity="HIGH",
        reason="5 failed logins within 5 minutes",
    )

    print("\nHIGH TEST")
    print(assessment.to_dict())

    decision = decide_response(
        assessment.level
    )

    print(decision.to_dict())

    assert assessment.level == "HIGH"
    assert decision.action == "STEP_UP_AUTHENTICATION"


def test_very_high():

    assessment = calculate_risk(
        fraud_type="ACCOUNT_CHANGE_NEW_DEVICE",
        severity="VERY_HIGH",
        reason="Sensitive change from new device",
    )

    print("\nVERY HIGH TEST")
    print(assessment.to_dict())

    decision = decide_response(
        assessment.level
    )

    print(decision.to_dict())

    assert assessment.level == "VERY_HIGH"
    assert decision.action == "TEMPORARY_RESTRICTION"


def test_critical():

    assessment = calculate_risk(
        fraud_type="ACCOUNT_TAKEOVER_SEQUENCE",
        severity="CRITICAL",
        reason="ATO sequence detected",
    )

    print("\nCRITICAL TEST")
    print(assessment.to_dict())

    decision = decide_response(
        assessment.level,
        incident_count=1,
    )

    print(decision.to_dict())

    assert assessment.level == "CRITICAL"
    assert decision.action == "TEMPORARY_PROTECTION"


def test_repeated_critical():

    decision = decide_response(
        "CRITICAL",
        incident_count=3,
    )

    print("\nREPEATED CRITICAL TEST")
    print(decision.to_dict())

    assert decision.action == "ADMIN_REVIEW"


if __name__ == "__main__":

    test_high()
    test_very_high()
    test_critical()
    test_repeated_critical()

    print("\n===================================")
    print("ALL RESPONSE-LAYER TESTS PASSED")
    print("===================================")