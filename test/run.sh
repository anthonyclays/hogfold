#!/bin/bash
#MQTT_TestCases.TC_MQTT_BROKER_CONNECT_005
docker run -v `pwd`:/config  -ti testware ttcn3_start iottestware.mqtt /config/config.cfg $1
