import os
import pandas as pd
from datetime import datetime
from db_connector import DBConnector

class DatabaseBackup(DBConnector):
    """데이터베이스 백업을 관리하는 클래스"""
    def __init__(self, backup_dir="backup"):
        super().__init__()
        # 입력받은 경로를 절대 경로로 변환
        self.backup_dir = os.path.abspath(backup_dir)

    def ensure_backup_directory(self):
        """백업 디렉토리 존재 여부 확인 및 생성"""
        try:
            # 상위 디렉토리 경로 가져오기
            parent_dir = os.path.dirname(self.backup_dir)

            # 상위 디렉토리가 존재하지 않는 경우
            if not os.path.exists(parent_dir):
                print(f"Error: 상위 디렉토리가 존재하지 않습니다 - {parent_dir}")
                return False

            # 백업 디렉토리가 없으면 생성
            if not os.path.exists(self.backup_dir):
                os.mkdir(self.backup_dir)  # makedirs 대신 mkdir 사용
                print(f"백업 디렉토리가 생성되었습니다: {self.backup_dir}")
            return True

        except Exception as e:
            print(f"백업 디렉토리 생성 실패: {e}")
            return False

    def set_filename(self, current_date):
        """중복되지 않는 백업 파일명 생성"""
        # 경로 구분자를 운영체제에 맞게 사용
        base_filename = os.path.join(self.backup_dir, f"backup_{current_date}.csv")

        if not os.path.exists(base_filename):
            return base_filename

        counter = 1
        while True:
            new_filename = os.path.join(self.backup_dir, f"backup_{current_date}_{counter}.csv")
            if not os.path.exists(new_filename):
                return new_filename
            counter += 1

    def backup(self):
        """데이터베이스 백업 실행"""
        try:
            # 백업 디렉토리 확인/생성
            if not self.ensure_backup_directory():
                print("백업을 진행할 수 없습니다.")
                return False

            # DB 연결 및 데이터 추출
            conn = self.connect()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT year, semester, agency_name, affiliation, agency_number, 
                       ele_school, mid_school, t_name, nice_number, birthday,
                       area, start_date, end_date, reg_date, remarks
                FROM TB_ELEARNING_DATA
            """)

            columns = [desc[0] for desc in cursor.description]
            data = pd.DataFrame(cursor.fetchall(), columns=columns)

            # 백업 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            backup_file = self.set_filename(current_date)

            # 데이터 저장
            data.to_csv(backup_file, index=False, encoding='utf-8-sig')

            print(f"백업이 성공적으로 완료되었습니다.")
            print(f"백업 파일 위치: {backup_file}")

            conn.close()
            return True

        except Exception as e:
            print(f"백업 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return False
