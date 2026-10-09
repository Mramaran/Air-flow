import serpapi_provider


def test_provider_info_names_the_hook_and_connection_type():
    info = serpapi_provider.get_provider_info()

    assert info["package-name"] == "airflow-provider-serpapi"
    assert info["versions"] == [serpapi_provider.__version__]
    (connection,) = info["connection-types"]
    assert connection["connection-type"] == "serpapi"
    assert connection["hook-class-name"] == "serpapi_provider.hooks.serpapi.SerpApiHook"
    assert connection["hook-name"] == "SerpApi"


def test_the_connection_form_shows_only_the_key_and_the_base_url():
    (connection,) = serpapi_provider.get_provider_info()["connection-types"]
    behaviour = connection["ui-field-behaviour"]

    assert behaviour["relabeling"] == {"password": "API key", "host": "Base URL (optional)"}
    assert set(behaviour["hidden-fields"]) == {"port", "schema", "login", "extra"}
