from app.domain.automation_policy import (
    AutomationPolicy,
    validate_policy_input,
    BALANCED_DEFAULTS,
)


def test_balanced_defaults_are_valid():
    policy = AutomationPolicy.balanced_defaults()
    errors = validate_policy_input(policy.to_dict(), partial=False)
    assert errors == []
    for key, value in BALANCED_DEFAULTS.items():
        assert getattr(policy, key) == value


def test_validate_rejects_out_of_range_cycle_interval():
    data = AutomationPolicy.balanced_defaults().to_dict()
    data["cycle_interval_minutes"] = 2
    errors = validate_policy_input(data, partial=False)
    assert any("cycle_interval_minutes" in e for e in errors)


def test_validate_rejects_bad_search_order():
    data = AutomationPolicy.balanced_defaults().to_dict()
    data["search_order"] = "chaotic"
    errors = validate_policy_input(data, partial=False)
    assert any("search_order" in e for e in errors)


def test_validate_rejects_non_bool_for_enabled_flags():
    data = AutomationPolicy.balanced_defaults().to_dict()
    data["missing_enabled"] = "yes"
    errors = validate_policy_input(data, partial=False)
    assert any("missing_enabled" in e for e in errors)


def test_validate_partial_only_checks_present_fields():
    errors = validate_policy_input({"hourly_api_cap": 5}, partial=True)
    assert errors == []
    errors = validate_policy_input({"hourly_api_cap": 0}, partial=True)
    assert any("hourly_api_cap" in e for e in errors)


def test_summary_mentions_key_facts():
    policy = AutomationPolicy.balanced_defaults()
    text = policy.summary()
    assert "missing items" in text
    assert "upgrades" in text
    assert "60 minute" in text
    assert "sequential" in text


def test_summary_reflects_disabled_modes():
    policy = AutomationPolicy(
        missing_enabled=False,
        upgrades_enabled=False,
        cycle_interval_minutes=60,
        hourly_api_cap=20,
        successful_grab_target=5,
        dispatch_interval_seconds=30,
        queue_target=10,
        cooldown_minutes=15,
        search_order="random",
    )
    text = policy.summary()
    assert "nothing" in text
    assert "random" in text
