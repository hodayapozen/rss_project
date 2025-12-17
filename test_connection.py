import boto3

def test_connection():
    try:
        # ניסיון ליצור אובייקט STS כדי לבדוק מי אני
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print("✅ החיבור הצליח!")
        print(f"User ID: {identity['UserId']}")
        print(f"Account: {identity['Account']}")
        
        # בדיקה אם יש גישה ל-S3
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        print(f"מצאתי {len(response['Buckets'])} באקטים בחשבון.")
        
    except Exception as e:
        print("❌ החיבור נכשל!")
        print(f"השגיאה: {e}")

if __name__ == "__main__":
    test_connection()