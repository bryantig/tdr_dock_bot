#!/usr/bin/env python3

import aws_cdk as cdk

from tdr_dock_bot.tdr_dock_bot_stack import TdrDockBotStack


app = cdk.App()
TdrDockBotStack(app, "tdr-dock-bot")

app.synth()
