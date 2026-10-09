"""Community Apache Airflow provider for SerpApi.

This is not an official SerpApi package.
"""

__version__ = "0.1.0"


def get_provider_info() -> dict:
    """Describe this package to Airflow. Named by the `apache_airflow_provider` entry point."""
    return {
        "package-name": "airflow-provider-serpapi",
        "name": "SerpApi",
        "description": "Community provider for SerpApi search data: batch searches, a quota guard and a connection type.",
        "versions": [__version__],
        "connection-types": [
            {
                "connection-type": "serpapi",
                "hook-class-name": "serpapi_provider.hooks.serpapi.SerpApiHook",
                "hook-name": "SerpApi",
                # Airflow 3.2 and later read the connection form's layout from here.
                # SerpApiHook.get_ui_field_behaviour() says the same for older versions.
                "ui-field-behaviour": {
                    "hidden-fields": ["port", "schema", "login", "extra"],
                    "relabeling": {"password": "API key", "host": "Base URL (optional)"},
                    "placeholders": {
                        "host": "https://serpapi.com",
                        "password": "Your SerpApi private key",
                    },
                },
            }
        ],
    }
