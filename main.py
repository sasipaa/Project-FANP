import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

# โหลดตัวแปร environment variables จากไฟล์ .env
load_dotenv()


def fetch_weather_forecast(api_token: str, domain: str, province: str, amphoe: str, start_time: str):
    # สร้าง URL API
    url = (
        f"https://data.tmd.go.th/nwpapi/v1/forecast/area/place"
        f"?domain={domain}&province={province}&amphoe={amphoe}"
        f"&fields=tc,rh&starttime={start_time}"
    )
    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {api_token}'
    }

    # ขอข้อมูลจาก API
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("ดึงข้อมูลสำเร็จแล้ว")
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
            columns=[
                'Latitude', 'Longitude', 'Time',
                'Temperature (°C)', 'Humidity (%)'
            ]
        )

        # บันทึกลงใน CSV
        csv_file = 'weather_forecast_data.csv'
        df.to_csv(csv_file, index=False)
        print(f"บันทึกข้อมูลลงใน '{csv_file}' เรียบร้อยแล้ว")
    else:
        print(f"ไม่สามารถดึงข้อมูลได้: {response.status_code}, {response.text}")

        if response.status_code == 422:
            print("กำลังทำการดึงข้อมูลจาก API อีกครั้งด้วยเวลาที่กำหนด โปรดรอสักครู่...")
            start_time = response.text.split('The starttime must be a date after or equal to ')[1].split('.')[0]

            print(f"กำหนดเวลาเริ่มต้นการดึงข้อมูลวันที่ {start_time}")
            fetch_weather_forecast(api_token, domain, province, amphoe, start_time)
        else:
            print(f"ไม่สามารถดึงข้อมูลได้: {response.status_code}, {response.text}")


# โหลด API Token จากตัวแปรสภาพแวดล้อม
TMD_API_TOKEN: str = os.getenv("TMD_API_TOKEN")

# ตัวอย่างการใช้งาน
if TMD_API_TOKEN:
    print("โหลด API Token สำเร็จแล้ว")
    # รับวันที่เวลาปัจจุบันในเวลา 08:00 น.
    now = datetime.now()
    # กำหนดให้เป็นวันที่ 2024-10-04 เวลา 08:00 น.
    start_time = now.replace(hour=8, minute=0, second=0, microsecond=0).isoformat()
    # start_time = now.replace(year=2024, month=10, day=4, hour=8, minute=0, second=0, microsecond=0).isoformat()


    print(f"กำหนดเวลาเริ่มต้นการดึงข้อมูลวันที่ {start_time}")
    fetch_weather_forecast(TMD_API_TOKEN, domain="2", province="พิษณุโลก", amphoe="เมืองพิษณุโลก", start_time=start_time)
else:
    print("ไม่พบโทเค็น API โปรดตรวจสอบไฟล์ .env ของคุณ")
