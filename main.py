import mariadb
import pandas as pd
import sys
from datetime import datetime
import re

def connect():
    try:
        conn = mariadb.connect(
            user='ielearning',
            password='pelearning!2345',
            host='127.0.0.1',
            port=3306,
            database='elearning_db'
        )
    except mariadb.Error as e:
        print(f"마리아DB와 연결 실패: {e}")
        sys.exit(1)
    return conn

def extract_year_semester(sheet_header):
    # 제목에서 연도와 학기를 추출하기 위한 표현식
    year_match = re.search(r'(\d{4})학년도', sheet_header)
    semester_match = re.search(r'(\d)학기', sheet_header)

    if not year_match:
        raise ValueError("시트 제목에서 연도를 추출할 수 없습니다.")
    if not semester_match:
        raise ValueError("시트 제목에서 학기를 추출할 수 없습니다.")

    year = int(year_match.group(1))
    semester = int(semester_match.group(1))

    return year, semester

def insert_data(conn, data, year, semester, ele_school, mid_school):
    cursor = conn.cursor()
    query = ("INSERT INTO TB_ELEARNING_DATA (year, semester, agency_name, affiliation, agency_number, ele_school, mid_school, t_name, nice_number, birthday, area, start_date, end_date, reg_date, remarks) "
             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    try:
        for _, row in data.iterrows():
            # 소속과 이름이 모두 빈칸인 경우 스킵
            if pd.isna(row['소속']) and pd.isna(row['성명']):
                continue

            try:
                agency_number = int(row['청번']) if '청번' in row and not pd.isna(row['청번']) else None
            except ValueError:
                agency_number = None

            cursor.execute(query, (
                year,
                semester,
                row['청명'] if '청명' in row and not pd.isna(row['청명']) else None,
                row['소속'] if '소속' in row and not pd.isna(row['소속']) else None,
                agency_number,
                ele_school,
                mid_school,
                row['성명'] if '성명' in row and not pd.isna(row['성명']) else None,
                row['나이스 개인번호'] if '나이스 개인번호' in row and not pd.isna(row['나이스 개인번호']) else None,
                row['생년월일-성별'] if '생년월일-성별' in row and not pd.isna(row['생년월일-성별']) else None,
                row['영역'] if '영역' in row and not pd.isna(row['영역']) else None,
                int(row['시작일']) if '시작일' in row and not pd.isna(row['시작일']) else None,
                int(row['종료일']) if '종료일' in row and not pd.isna(row['종료일']) else None,
                datetime.now(),
                row['비고'] if '비고' in row and not pd.isna(row['비고']) else None
            ))
        conn.commit()
    except mariadb.Error as e:
        print(f"Error: {e}")
        conn.rollback()

def main():
    # 엑셀 파일 경로
    excel_file_path = "C:\\Users\\huintech\\Desktop\\test.xlsx"

    # 엑셀 파일 읽기
    try:
        excel_file = pd.ExcelFile(excel_file_path, engine='openpyxl')
    except FileNotFoundError:
        print("Error: The specified file was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading the Excel file: {e}")
        sys.exit(1)

    for sheet_name in excel_file.sheet_names:
        # 시트의 첫 번째 셀에서 year와 semester 추출
        data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', header=None)
        sheet_header = str(data.iloc[0, 0])  # 첫 번째 셀의 값을 시트 제목으로 사용
        try:
            year, semester = extract_year_semester(sheet_header)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        # 실제 데이터는 다섯 번째 행 이후부터 시작
        data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', skiprows=4)

        # 열 이름 변경
        data.columns = ['연번', '청번', '청명', '소속', '성명', '나이스 개인번호', '생년월일-성별', '영역', '시작일', '종료일', '비고']

        # ele_school과 mid_school 설정
        ele_school = '초' if sheet_name == '초' else None
        mid_school = '중' if sheet_name == '중' else None

        # DB 연결
        conn = connect()

        # 데이터 삽입
        insert_data(conn, data, year, semester, ele_school, mid_school)

        # 연결 종료
        conn.close()
        print(f"Data insertion complete for sheet '{sheet_name}' and connection closed.")

if __name__ == "__main__":
    main()
