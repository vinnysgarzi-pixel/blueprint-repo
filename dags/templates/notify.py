"""Notification Blueprint templates.

Blueprints:
    - ``send_slack_notification`` -> SendSlackNotification
"""

from __future__ import annotations

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class SendSlackNotificationConfig(BaseModel):
    slack_webhook_conn_id: str = Field(
        default="slack_default",
        description="Airflow connection ID holding the Slack incoming webhook.",
    )
    message: str = Field(
        description="Message text. Supports Jinja, e.g. "
        "`Pipeline finished for {{ ds }}`.",
    )
    channel: str | None = Field(
        default=None,
        description="Override the channel the webhook posts to (optional).",
    )
    username: str | None = Field(
        default=None,
        description="Override the display name of the sender (optional).",
    )


class SendSlackNotification(Blueprint[SendSlackNotificationConfig]):
    """Post a message to Slack via an incoming webhook.

    The classic final "glue" step. Set `trigger_rule: all_done` (or
    `one_failed`) on the YAML step to turn it into a failure alert.
    """

    def render(self, config: SendSlackNotificationConfig) -> TaskOrGroup:
        kwargs = {
            "task_id": self.step_id,
            "slack_webhook_conn_id": config.slack_webhook_conn_id,
            "message": config.message,
        }
        if config.channel:
            kwargs["channel"] = config.channel
        if config.username:
            kwargs["username"] = config.username
        return SlackWebhookOperator(**kwargs)
