from serpapi_provider.core.quota import QuotaStatus, check_quota


def status(left=250, used=0, limit=50):
    return QuotaStatus(searches_left=left, used_this_hour=used, hourly_limit=limit)


def test_enough_this_month_and_this_hour_is_ok():
    decision = check_quota(status(), wanted=20)

    assert decision.ok
    assert "20 searches wanted" in decision.reason


def test_a_monthly_shortfall_is_refused_with_the_numbers():
    decision = check_quota(status(left=12), wanted=20)

    assert not decision.ok
    assert "wants 20" in decision.reason and "has 12 left" in decision.reason


def test_the_reserve_is_kept_back():
    assert check_quota(status(left=30), wanted=20).ok

    decision = check_quota(status(left=30), wanted=20, reserve=15)

    assert not decision.ok
    assert "keeping 15 in reserve" in decision.reason and "15 usable" in decision.reason


def test_an_hourly_shortfall_is_refused_with_the_numbers():
    decision = check_quota(status(used=40), wanted=20)

    assert not decision.ok
    assert "only 10 are left this hour" in decision.reason and "40 of 50 used" in decision.reason


def test_exactly_enough_is_ok():
    assert check_quota(status(left=20, used=30), wanted=20).ok


def test_wanting_nothing_is_always_ok():
    assert check_quota(status(left=0, used=50), wanted=0).ok


def test_from_account_prefers_total_searches_left():
    parsed = QuotaStatus.from_account(
        {
            "plan_searches_left": 100,
            "total_searches_left": 130,
            "this_hour_searches": 7,
            "account_rate_limit_per_hour": 50,
        }
    )

    assert parsed == QuotaStatus(searches_left=130, used_this_hour=7, hourly_limit=50)
    assert parsed.left_this_hour == 43


def test_from_account_falls_back_to_plan_searches_left_and_zero():
    assert QuotaStatus.from_account({"plan_searches_left": 100}) == QuotaStatus(100, 0, 0)
    assert QuotaStatus.from_account({}) == QuotaStatus(0, 0, 0)


def test_left_this_hour_never_goes_negative():
    assert status(used=60).left_this_hour == 0
