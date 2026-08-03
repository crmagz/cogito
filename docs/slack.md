# Slack notifications

Cogito can post outbound-only, Slack-native Block Kit notifications. Under
normal delivery, one root message is created for each Cogito run; later
lifecycle and approval events are replies in that message's thread. Each
message has a URL button to the authenticated Workbench. Slack cannot approve,
reject, revise, or otherwise signal a Cogito workflow.

Delivery is at-least-once. Cogito persists a per-run thread identity and a
per-event message mapping, but an external Slack acknowledgement and a database
commit cannot be atomic. A process failure in that narrow interval can cause a
retry and a duplicate Slack message; it never changes workflow or approval
state.

## Create a private Slack app

In your personal Slack workspace, create an app **from scratch** and install it
only into that workspace.

1. Under **OAuth & Permissions**, add the sole Bot Token Scope: `chat:write`.
2. Install the app to the workspace and copy its Bot User OAuth Token. Treat
   the token as a password; do not paste it into Git, Helm values, or terminal
   history.
3. Create or choose the notification channel, then invite the bot to that
   channel. The bot needs membership to post there.
4. Copy the channel ID from its Slack link. It is the final `C...` segment of
   a URL such as `https://<workspace>.slack.com/archives/C01234567`.

No Slack interactivity, event subscription, slash command, OAuth redirect, or
request URL is required for this phase.

## Configure Cogito

Store the Bot User OAuth Token in a Kubernetes Secret managed by your normal
secret-management workflow. The secret's token key defaults to `bot-token`.
Use a private Helm values file, never a tracked values file:

```yaml
api:
  notifications:
    enabled: true
    provider: slack
    slackChannelId: C01234567
    slackWorkbenchUrl: https://workbench.example.test
    slackExistingSecret: cogito-slack
    slackBotTokenKey: bot-token
```

`slackWorkbenchUrl` must be the public HTTPS base URL of the Workbench. Cogito
uses it to construct `/runs/<run-id>/workflow` links. The bot token is mounted
only as `COGITO_NOTIFICATION_SLACK_BOT_TOKEN`; it is not written to the API
ConfigMap.

After rendering and reviewing the deployment, start one disposable Cogito run.
Confirm that Slack receives one root message and a later reply in the same
thread, then use the URL button to confirm the Workbench still enforces its
normal session and project scope.
