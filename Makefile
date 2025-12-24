PROJECT = rabbitmq_delayed_message_exchange
PROJECT_DESCRIPTION = RabbitMQ Delayed Message Exchange
PROJECT_MOD = rabbit_delayed_message_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, ["4.2.0"]}
endef

DEPS = rabbit_common rabbit khepri aws_erlang
TEST_DEPS = ct_helper rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client
dep_ct_helper = git https://github.com/extend/ct_helper.git master
dep_aws_erlang = git https://github.com/aws-beam/aws-erlang.git master

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
