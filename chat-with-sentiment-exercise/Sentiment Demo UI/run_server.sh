#!/bin/sh
echo "${Quix__Workspace__Id}" > /usr/share/nginx/html/workspace_id
echo "${Quix__Sdk__Token}" > /usr/share/nginx/html/sdk_token
echo "${Quix__Portal__Api}" > /usr/share/nginx/html/portal_api
echo "${sentiment}" > /usr/share/nginx/html/sentiment_topic
echo "${messages}" > /usr/share/nginx/html/messages_topic
echo "${drafts}" > /usr/share/nginx/html/drafts_topic
echo "${drafts_sentiment}" > /usr/share/nginx/html/drafts_sentiment_topic
echo "${messages_sanitized}" > /usr/share/nginx/html/messages_sanitized_topic
nginx -g "daemon off;"