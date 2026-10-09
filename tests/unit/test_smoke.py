import serpapi_provider


def test_provider_info_names_the_hook_and_connection_type():
    info = serpapi_provider.get_provider_info()

    assert info["package-name"] == "airflow-provider-serpapi"
    assert info["versions"] == [serpapi_provider.__version__]
    assert info["connection-types"] == [
        {
            "connection-type": "serpapi",
            "hook-class-name": "serpapi_provider.hooks.serpapi.SerpApiHook",
        }
    ]
