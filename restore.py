from db_connector import DBConnector
import pandas as pd
import os
from datetime import datetime

class DatabaseRestore(DBConnector):
    """데이터베이스 복원을 관리하는 클래스"""
    def __init__(self, backup_dir="backup"):
        super().__init__()
        self.backup_dir = backup_dir

    def get_backup_files(self, file_pattern=None):
        """사용 가능한 백업 파일 목록 조회"""
        if not os.path.exists(self.backup_dir):
            print(f"백업 폴더({self.backup_dir})가 존재하지 않습니다.")
            return []

        all_files = os.listdir(self.backup_dir)

        if file_pattern:
            backup_files = [f for f in all_files if f.startswith(file_pattern) and f.endswith('.csv')]
        else:
            backup_files = [f for f in all_files if f.endswith('.csv')]

        if not backup_files:
            pattern_msg = f"패턴 '{file_pattern}'과 일치하는 " if file_pattern else ""
            print(f"폴더에 {pattern_msg}CSV 파일이 없습니다.")
            return []

        return sorted(backup_files, reverse=True)

    def restore(self, backup_file):
        """선택된 백업 파일로 데이터베이스 복원"""
        try:
            backup_path = os.path.join(self.backup_dir, backup_file)

            if not os.path.exists(backup_path):
                print(f"오류: 파일을 찾을 수 없습니다 - {backup_path}")
                return

            print(f"백업 파일을 읽는 중: {backup_file}")
            data = pd.read_csv(backup_path)

            required_columns = ['year', 'semester', 'agency_name', 'affiliation',
                                'agency_number', 'ele_school', 'mid_school', 't_name',
                                'nice_number', 'birthday', 'area', 'start_date',
                                'end_date', 'reg_date', 'remarks']

            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                print(f"오류: 필수 컬럼이 누락되었습니다 - {', '.join(missing_columns)}")
                return

            conn = self.connect()
            cursor = conn.cursor()

            print("\n경고: 이 작업은 현재 데이터베이스의 모든 데이터를 삭제하고 백업 데이터로 교체합니다.")
            confirm = input("계속 진행하시겠습니까? (y/n): ")
            if confirm.lower() != 'y':
                print("복구 작업이 취소되었습니다.")
                conn.close()
                return

            cursor.execute("TRUNCATE TABLE TB_ELEARNING_DATA")

            insert_query = """
                INSERT INTO TB_ELEARNING_DATA 
                (year, semester, agency_name, affiliation, agency_number, 
                 ele_school, mid_school, t_name, nice_number, birthday,
                 area, start_date, end_date, reg_date, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            for _, row in data.iterrows():
                values = []
                for value in row:
                    if pd.isna(value):
                        values.append(None)
                    else:
                        if isinstance(value, str) and 'reg_date' in data.columns[len(values)]:
                            try:
                                values.append(datetime.strptime(value, '%Y-%m-%d %H:%M:%S'))
                            except ValueError:
                                values.append(None)
                        else:
                            values.append(value)

                cursor.execute(insert_query, values)

            conn.commit()
            print(f"\n복구가 성공적으로 완료되었습니다.")
            print(f"복구된 레코드 수: {len(data)}")

        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
            print(f"복구 중 오류 발생: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
