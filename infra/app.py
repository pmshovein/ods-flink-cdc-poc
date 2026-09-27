#!/usr/bin/env python3
from aws_cdk import App, Environment

from infra.constants import OdsEnvironment
from infra.ods_flink_stack import OdsFlinkStack


app = App()
config = OdsEnvironment.from_environment()
OdsFlinkStack(
    app,
    f"OdsFlink-{config.name}",
    config=config,
    env=Environment(account=config.account, region=config.region),
)
app.synth()

