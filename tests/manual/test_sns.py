from etl.utils.sns_alerts import publish_alert

if __name__ == "__main__":
    publish_alert("Test SNS", "SNS is working!")
    print("SNS test sent — check your email!")
