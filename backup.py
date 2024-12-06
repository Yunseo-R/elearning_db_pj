import os
import pandas as pd
from datetime import datetime
from db_connector import DBConnector

class DatabaseBackup(DBConnector):
    """
    elearning_db 데이터베이스의 데이터를 CSV 파일로 백업하고,
    백업 이력을 관리하는 클래스.
    """

    def __init__(self):
        """DatabaseBackup 클래스 초기화"""
        super().__init__()

    def backup_to_csv(self, file_path: str) -> bool:
        """
        tb_elearning_data 테이블의 데이터를 CSV 파일로 백업하는 메서드.

        Args:
            file_path (str): 백업 파일을 저장할 경로. 사용자 지정 경로가 팝업으로 설정됨.

        Returns:
            bool: 백업 성공 여부. True면 성공, False면 실패.
        """
        status = 0  # 초기 상태: 처리중
        try:
            # 데이터베이스 연결 및 데이터 읽기
            conn = self.connect()
            cursor = conn.cursor()

            # tb_elearning_data 테이블에서 데이터 조회
            cursor.execute("""
                SELECT year, semester, agency_name, affiliation, agency_number, 
                       school_type, t_name, nice_number, birthday,
                       area, start_date, end_date, reg_date, remarks
                FROM tb_elearning_data
            """)
            columns = [desc[0] for desc in cursor.description]  # 컬럼 이름 가져오기
            data = pd.DataFrame(cursor.fetchall(), columns=columns)

            # CSV 파일로 저장
            data.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"백업이 성공적으로 완료되었습니다. 파일 경로: {file_path}")

            # 백업 성공 상태로 로그 저장
            status = 1
            self.save_backup_log(file_path, status)

            conn.close()
            return True
        except Exception as e:
            print(f"백업 중 오류 발생: {e}")

            # 백업 실패 상태로 로그 저장
            status = 2
            self.save_backup_log(file_path, status)

            if 'conn' in locals():
                conn.close()
            return False

    def save_backup_log(self, file_path: str, status: int):
        """
        tb_backup_data 테이블에 백업 정보를 저장하는 메서드.

        Args:
            file_path (str): 백업 파일 경로.
            status (int): 백업 상태 (0: 대기 또는 처리중, 1: 백업 완료, 2: 백업 실패).
        """
        try:
            # 데이터베이스 연결
            conn = self.connect()
            cursor = conn.cursor()

            # 현재 시간 가져오기
            current_time = datetime.now()

            # tb_backup_data 테이블에 백업 정보 삽입
            cursor.execute("""
                INSERT INTO tb_backup_data (backup_date, file_path, status)
                VALUES (?, ?, ?)
            """, (current_time, file_path, status))
            conn.commit()

            conn.close()
            print(f"백업 정보가 저장되었습니다. 경로: {file_path}, 상태: {status}")
        except Exception as e:
            print(f"백업 정보 저장 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()

    def fetch_backup_history(self):
        """
        tb_backup_data 테이블에서 백업 이력을 조회하는 메서드.

        Returns:
            List[Dict]: 백업 이력 목록. 각 이력은 딕셔너리 형태로 반환.
                - dat_idx (int): 백업 데이터 ID.
                - backup_date (datetime): 백업 실행 날짜 및 시간.
                - file_path (str): 백업 파일 경로.
                - status (str): 백업 상태 ('대기', '백업 완료', '백업 실패').
        """
        try:
            # 데이터베이스 연결
            conn = self.connect()
            cursor = conn.cursor()

            # 백업 이력 조회
            cursor.execute("""
                SELECT dat_idx, backup_date, file_path, status
                FROM tb_backup_data
                ORDER BY backup_date DESC
            """)
            columns = [desc[0] for desc in cursor.description]

            # 상태값 변환 및 데이터 정리
            results = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                if record['status'] == 0:
                    record['status'] = "대기"
                elif record['status'] == 1:
                    record['status'] = "백업 완료"
                elif record['status'] == 2:
                    record['status'] = "백업 실패"
                results.append(record)

            conn.close()
            return results
        except Exception as e:
            print(f"백업 이력 조회 중 오류 발생: {e}")
            if 'conn' in locals():
                conn.close()
            return []

