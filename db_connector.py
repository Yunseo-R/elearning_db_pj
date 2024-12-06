import mariadb
import sys

class DBConnector:
    """
    데이터베이스 연결을 관리하는 기본 클래스
    이 클래스는 MariaDB 연결에 필요한 설정들을 관리하고 DB 연결을 생성합니다.
    다른 DB관련 클래스들의 기본 클래스로 사용됩니다.

    다른 클래스에서 import할 경우 기본값을 사용하지 않으려면 매개변수를 변경하세요.
    사용 예시:
        # 기본 설정으로 연결
        # 기본값은 '화면정의서' 기준
        db = DBConnector()

        # 커스텀 설정으로 연결
        custom_db = DBConnector(
            user='custom_user',
            password='custom_pass',
            host='192.168.1.100',
            port=3307,
            database='custom_db'
        )

        # 일부 설정만 변경
        local_db = DBConnector(database='other_db')
    """
    def __init__(self, user='ielearning', password='pelearning!2345',
                 host='127.0.0.1', port=3306, database='elearning_db'):
        """
        DBConnector 클래스의 초기화 메서드
    Args:
        user (str): 데이터베이스 접속 계정명. 기본값은 'ielearning'
        password (str): 데이터베이스 접속 비밀번호. 기본값은 'pelearning!2345'
        host (str): 데이터베이스 서버 주소. 기본값은 '127.0.0.1' (로컬호스트)
        port (int): 데이터베이스 서버 포트 번호. 기본값은 3306
        database (str): 사용할 데이터베이스 이름. 기본값은 'elearning_db'

    각 매개변수는 인스턴스 변수로 저장되어 connect() 메서드에서 사용됩니다.
    기본값 이외의 값을 설정하려면 위의 사용 예시를 참고하세요.
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def connect(self):
        """
        데이터베이스 연결을 설정하고 연결 객체를 반환하는 메서드
        DB와 연결해, SQL 쿼리 실행과 트랜젝션 관리에 사용할 수 있는 연결 객체 conn를 반환합니다.
        DB와 연결에 실패할 시 에러메세지를 출력하고 프로그램을 종료합니다.

        Returns:
            mariadb.connection: MariaDB 데이터베이스 연결 객체
        Raises:
            mariadb.Error: DB 연결 실패 시 발생하는 예외. 발생 시 프로그램 종료됨

        """
        try:
            conn = mariadb.connect(
                user=self.user,      # DB 접속 계정명
                password=self.password,  # DB 접속 비밀번호
                host=self.host,      # DB 서버 주소
                port=self.port,      # DB 서버 포트
                database=self.database   # 사용할 DB 이름
            )
            return conn
        except mariadb.Error as e:
            print(f"마리아DB와 연결 실패: {e}")
            sys.exit(1)  # 연결 실패 시 프로그램 종료 (종료 코드 1)
