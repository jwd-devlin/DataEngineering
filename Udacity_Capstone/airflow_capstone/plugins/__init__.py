from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.CreateTablesOperator,
        operators.USCityCoordinatesOperator,
        operators.USDemographicsOperator,
        operators.USAirportsOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.DataStorage,
        helpers.DataLocations
    ]

