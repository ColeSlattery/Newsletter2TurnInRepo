import os
import requests

def send_simple_message_templates():
	return requests.post(
		"https://api.m.mailgun.org/messages",
		auth=("api", "MJNJNJ"),
		data={"from": "Mailgun Sandbox <postmaster@.mailgun.org>",
			"to": "Cole Slattery <cosl2760@colorado.edu>",
			"subject": "Hello Cole Slattery",
			"template": "test newsletter outline",
			"h:X-Mailgun-Variables": '{"test": "test"}'})

if __name__ == "__main__":
    print("Sending email via Mailgun...")
    response = send_simple_message_templates()
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
