from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import requests
import pandas as pd
from dotenv import load_dotenv

# ฟังก์ชันสำหรับโหลด API Token
def load_api_token(**kwargs):
    load_dotenv(dotenv_path="/opt/airflow/.env")  # ระบุ path ไปยังไฟล์ .env
    TMD_API_TOKEN = os.getenv("TMD_API_TOKEN")
    
    print(f"Loaded API Token: {TMD_API_TOKEN}")
    
    if not TMD_API_TOKEN:
        raise Exception("API Token is missing or not loaded.")
    
    # ส่ง API Token กลับไปยัง context ของ DAG
    kwargs['ti'].xcom_push(key='api_token', value=TMD_API_TOKEN)

# ฟังก์ชันดึงข้อมูลพยากรณ์อากาศ
def fetch_weather_forecast(**kwargs):
    # ดึง API Token จาก XCom
    api_token = kwargs['ti'].xcom_pull(key='api_token', task_ids='load_api_token')
    
    domain = kwargs['domain']
    province = kwargs['province']
    amphoe = kwargs['amphoe']

    # กำหนด start_time เป็น execution_date เวลา 14:00 น. หรือมากกว่า
    start_time = kwargs['execution_date'].replace(hour=14, minute=0, second=0, microsecond=0)  # เวลา 14:00 น.
    start_time_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S')  # ใช้เวลาที่กำหนดในรูปแบบ ISO

    # Debugging output
    print(f"Domain: {domain}, Province: {province}, Amphoe: {amphoe}, Start Time: {start_time_iso}")

    # สร้าง URL API
    url = (
        f"https://data.tmd.go.th/nwpapi/v1/forecast/area/place"
        f"?domain={domain}&province={province}&amphoe={amphoe}"
        f"&fields=tc,rh&starttime={start_time_iso}"
    )
    print(f"Request URL: {url}")  # Print the URL to check

    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {api_token}'
    }

    # ขอข้อมูลจาก API
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        # การแยกและประมวลผลข้อมูลพยากรณ์อากาศ
        weather_forecast = [
            [
                item['location']['lat'],
                item['location']['lon'],
                forecast['time'],
                forecast['data']['tc'],
                forecast['data']['rh']
            ]
            for item in data['WeatherForecasts']
            for forecast in item['forecasts']
        ]

        # สร้าง DataFrame
        df = pd.DataFrame(
            weather_forecast,
            columns=['Latitude', 'Longitude', 'Time', 'Temperature (°C)', 'Humidity (%)']
        )

        # สร้างชื่อไฟล์ตามวันที่
        today = datetime.now().strftime('%Y-%m-%d')  # ใช้วันที่ปัจจุบัน
        csv_file = f'/home/airflow/data/weather_forecast_data_{today}.csv'  # ตั้งชื่อไฟล์เป็น weather_forecast_data_date.csv

        # บันทึกลงใน CSV
        df.to_csv(csv_file, index=False)
        print(f"บันทึกข้อมูลลงใน '{csv_file}' เรียบร้อยแล้ว")
    else:
        print(f"ไม่สามารถดึงข้อมูลได้: {response.status_code}, {response.text}")
        raise Exception(f"Failed to fetch data: {response.status_code}")

# กำหนดค่าเริ่มต้นของ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# สร้าง DAG
with DAG(
    'weather_forecast_dag',
    default_args=default_args,
    description='ดึงข้อมูลพยากรณ์อากาศจาก TMD API',
    schedule_interval='0 */6 * * *',  # รันทุกๆ 6 ชม
    catchup=False,  # ไม่ต้องรันย้อนหลัง
) as dag:

    # Task สำหรับโหลด API Token
    load_api_task = PythonOperator(
        task_id='load_api_token',
        python_callable=load_api_token,
        provide_context=True,
    )

    task_fetch_weather = PythonOperator(
        task_id='fetch_weather_forecast',
        python_callable=fetch_weather_forecast,
        op_kwargs={
            'domain': '2',
            'province': 'พิษณุโลก',
            'amphoe': 'เมืองพิษณุโลก',
            # 'start_time' จะถูกกำหนดภายในฟังก์ชัน
        },
    )

    # กำหนดลำดับการทำงาน
    load_api_task >> task_fetch_weather
