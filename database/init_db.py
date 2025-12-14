"""
Скрипт для инициализации базы данных PostgreSQL
Использование: python database/init_db.py
"""
import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from dateutil.relativedelta import relativedelta
import random

# Устанавливаем кодировку для Windows консоли
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Добавляем корневую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from werkzeug.security import generate_password_hash
from config import config
from models import Base, Client, Loan, UnclaimedItem, Sale, InterestRate, Employee, async_session_maker, engine
from sqlalchemy import select, func, create_engine, text
from sqlalchemy.orm import sessionmaker

# Списки для генерации данных (разделены по полу)
MALE_FIRST_NAMES = [
    'Александр', 'Дмитрий', 'Максим', 'Сергей', 'Андрей', 'Алексей', 'Артем', 'Илья',
    'Кирилл', 'Михаил', 'Никита', 'Матвей', 'Роман', 'Егор', 'Арсений', 'Иван',
    'Денис', 'Евгений', 'Данил', 'Тимур', 'Владислав', 'Игорь', 'Владимир', 'Павел',
    'Антон', 'Борис', 'Василий', 'Виктор', 'Геннадий', 'Григорий', 'Дмитрий', 'Евгений'
]

FEMALE_FIRST_NAMES = [
    'Анна', 'Мария', 'Елена', 'Ольга', 'Татьяна', 'Наталья', 'Ирина', 'Светлана',
    'Екатерина', 'Юлия', 'Анастасия', 'Дарья', 'Валентина', 'Оксана', 'Анжела', 'Надежда',
    'Людмила', 'Галина', 'Валентина', 'Лариса', 'Елена', 'Марина', 'Анна', 'Тамара',
    'Вера', 'Нина', 'Зинаида', 'Раиса', 'Лидия', 'Василиса', 'Антонина', 'Клавдия'
]

# Фамилии в мужском роде
MALE_LAST_NAMES = [
    'Иванов', 'Петров', 'Смирнов', 'Козлов', 'Попов', 'Соколов', 'Лебедев', 'Новиков',
    'Морозов', 'Волков', 'Соловьев', 'Васильев', 'Зайцев', 'Павлов', 'Семенов',
    'Голубев', 'Виноградов', 'Богданов', 'Воробьев', 'Федоров', 'Михайлов', 'Белов',
    'Тарасов', 'Беляев', 'Комаров', 'Орлов', 'Киселев', 'Макаров', 'Андреев', 'Ковалев',
    'Степанов', 'Николаев', 'Орлов', 'Андреев', 'Алексеев', 'Романов', 'Владимиров'
]

# Фамилии в женском роде (соответствующие мужским)
FEMALE_LAST_NAMES = [
    'Иванова', 'Петрова', 'Смирнова', 'Козлова', 'Попова', 'Соколова', 'Лебедева', 'Новикова',
    'Морозова', 'Волкова', 'Соловьева', 'Васильева', 'Зайцева', 'Павлова', 'Семенова',
    'Голубева', 'Виноградова', 'Богданова', 'Воробьева', 'Федорова', 'Михайлова', 'Белова',
    'Тарасова', 'Беляева', 'Комарова', 'Орлова', 'Киселева', 'Макарова', 'Андреева', 'Ковалева',
    'Степанова', 'Николаева', 'Орлова', 'Андреева', 'Алексеева', 'Романова', 'Владимирова'
]

MALE_MIDDLE_NAMES = [
    'Александрович', 'Дмитриевич', 'Максимович', 'Сергеевич', 'Андреевич', 'Алексеевич',
    'Артемович', 'Ильич', 'Кириллович', 'Михайлович', 'Никитич', 'Матвеевич', 'Романович',
    'Егорович', 'Арсеньевич', 'Иванович', 'Денисович', 'Евгеньевич', 'Данилович',
    'Владимирович', 'Викторович', 'Геннадьевич', 'Григорьевич', 'Борисович', 'Антонович'
]

FEMALE_MIDDLE_NAMES = [
    'Александровна', 'Дмитриевна', 'Максимовна', 'Сергеевна', 'Андреевна', 'Алексеевна',
    'Артемовна', 'Ильинична', 'Кирилловна', 'Михайловна', 'Никитична', 'Матвеевна',
    'Романовна', 'Егоровна', 'Арсеньевна', 'Ивановна', 'Денисовна', 'Евгеньевна',
    'Даниловна', 'Владимировна', 'Викторовна', 'Геннадьевна', 'Григорьевна', 'Борисовна'
]

# Товары с примерными ценами (в рублях)
GOODS_DATA = {
    'Драгоценности': [
        ('Кольцо с рубином', 50000, 200000),
        ('Золотая цепочка', 30000, 150000),
        ('Бриллиантовое колье', 200000, 1000000),
        ('Золотые серьги', 25000, 120000),
        ('Серебряный сервиз', 40000, 180000),
        ('Наручные часы', 20000, 300000),
        ('Золотой браслет', 35000, 140000)
    ],
    'Электроника': [
        ('Ноутбук', 30000, 150000),
        ('Смартфон', 15000, 100000),
        ('Планшет', 20000, 80000),
        ('Игровая консоль', 25000, 60000),
        ('Телевизор', 20000, 200000),
        ('Ноутбук игровой', 50000, 200000)
    ],
    'Товары для туризма': [
        ('Палатка', 5000, 30000),
        ('Велосипед', 10000, 80000),
        ('Скутер', 15000, 100000)
    ],
    'Предмет коллекционирования': [
        ('Коллекционные монеты', 10000, 100000),
        ('Антикварная ваза', 30000, 200000),
        ('Старинная книга', 5000, 50000),
        ('Марки', 2000, 30000)
    ],
    'Предмет роскоши': [
        ('Дизайнерская сумка', 20000, 150000),
        ('Картина', 50000, 500000),
        ('Скульптура', 30000, 300000)
    ],
    'Мототранспорт': [
        ('Мотоцикл', 50000, 500000)
    ],
    'Автотранспорт': [
        ('Автомобиль', 200000, 5000000)
    ],
    'Бытовая техника': [
        ('Холодильник', 15000, 100000),
        ('Стиральная машина', 10000, 80000),
        ('Микроволновка', 3000, 20000),
        ('Кофемашина', 15000, 100000)
    ],
    'Мебель': [
        ('Мебель', 10000, 200000)
    ],
    'Одежда': [
        ('Одежда', 2000, 50000)
    ],
    'Обувь': [
        ('Обувь', 1000, 30000)
    ],
    'Аксессуары': [
        ('Аксессуары', 1000, 20000)
    ]
}

PHYSICAL_CONDITIONS = ['Отличное', 'Хорошее', 'Удовлетворительное', 'Плохое', 'Ужасное']

POSITIONS = ['Оценщик-товаровед', 'Менеджер-товаровед', 'Менеджер по продажам']

LOAN_STATUSES = ['Активен', 'Выплачен', 'Просрочен']

async def create_tables():
    """Создание всех таблиц"""
    print("Создание таблиц...")
    
    # Удаление таблиц если они существуют (с CASCADE для удаления зависимых объектов)
    async with engine.begin() as conn:
        # Сначала удаляем таблицу логов, если она существует (она создается триггером)
        try:
            await conn.execute(text('DROP TABLE IF EXISTS "Лог_изменений_займов" CASCADE'))
        except Exception:
            pass  # Игнорируем ошибки, если таблицы нет
        
        # Удаляем все таблицы с использованием CASCADE через raw SQL
        # Получаем список всех таблиц и удаляем их с CASCADE
        result = await conn.execute(text("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
        """))
        tables = result.fetchall()
        
        # Удаляем каждую таблицу с CASCADE
        for table in tables:
            try:
                await conn.execute(text(f'DROP TABLE IF EXISTS "{table[0]}" CASCADE'))
            except Exception:
                pass
        
        # Создаем таблицы заново
        await conn.run_sync(Base.metadata.create_all)
    
    print("[OK] Таблицы созданы")

async def install_sql_scripts():
    """Установка SQL триггеров, функций и процедур - отключено"""
    pass

async def generate_interest_rates():
    """Генерация процентов по займу"""
    print("Генерация процентов по займу...")
    
    rates = []
    conditions = [5.00, 7.50, 10.00, 12.50, 15.00]
    terms = [5.00, 6.25, 7.50, 8.75, 10.00]
    
    index = 1
    for condition in conditions:
        for term in terms:
            rate = InterestRate(
                Индекс_процента=index,
                Состояние_товара=Decimal(str(condition)),
                Срок_займа=Decimal(str(term)),
                Процент=Decimal(str(condition + term))
            )
            rates.append(rate)
            index += 1
    
    async with async_session_maker() as session:
        session.add_all(rates)
        await session.commit()
    print(f"[OK] Создано {len(rates)} записей в таблице Процент_по_займу")

async def generate_clients(count=50):
    """Генерация клиентов"""
    print(f"Генерация {count} клиентов...")
    
    clients = []
    used_phones = set()
    
    for i in range(1, count + 1):
        # Генерация уникального телефона
        while True:
            phone = f"7{random.randint(9000000000, 9999999999)}"
            if phone not in used_phones:
                used_phones.add(phone)
                break
        
        # Определяем пол случайно (50/50)
        is_male = random.choice([True, False])
        
        if is_male:
            first_name = random.choice(MALE_FIRST_NAMES)
            middle_name = random.choice(MALE_MIDDLE_NAMES)
            last_name = random.choice(MALE_LAST_NAMES)
        else:
            first_name = random.choice(FEMALE_FIRST_NAMES)
            middle_name = random.choice(FEMALE_MIDDLE_NAMES)
            last_name = random.choice(FEMALE_LAST_NAMES)
        
        fio = f"{last_name} {first_name} {middle_name}"
        
        client = Client(
            ID_Клиента=i,
            ФИО=fio,
            Телефон=phone
        )
        clients.append(client)
    
    async with async_session_maker() as session:
        session.add_all(clients)
        await session.commit()
    print(f"[OK] Создано {len(clients)} клиентов")

async def generate_employees(count=45):
    """Генерация сотрудников"""
    print(f"Генерация {count} сотрудников...")
    
    employees = []
    used_phones = set()
    
    for i in range(1, count + 1):
        # Генерация уникального телефона
        while True:
            phone = f"7{random.randint(9000000000, 9999999999)}"
            if phone not in used_phones:
                used_phones.add(phone)
                break
        
        # Определяем пол случайно (50/50)
        is_male = random.choice([True, False])
        
        if is_male:
            first_name = random.choice(MALE_FIRST_NAMES)
            middle_name = random.choice(MALE_MIDDLE_NAMES)
            last_name = random.choice(MALE_LAST_NAMES)
        else:
            first_name = random.choice(FEMALE_FIRST_NAMES)
            middle_name = random.choice(FEMALE_MIDDLE_NAMES)
            last_name = random.choice(FEMALE_LAST_NAMES)
        
        fio = f"{last_name} {first_name} {middle_name}"
        
        position = random.choice(POSITIONS)
        
        # Дата приема от 2020 до текущей даты
        today = date.today()
        min_year = 2020
        max_year = today.year
        
        hire_year = random.randint(min_year, max_year)
        if hire_year == max_year:
            hire_month = random.randint(1, min(today.month, 12))
            if hire_month == today.month:
                hire_day = random.randint(1, min(today.day, 28))
            else:
                hire_day = random.randint(1, 28)
        else:
            hire_month = random.randint(1, 12)
            hire_day = random.randint(1, 28)
        hire_date = date(hire_year, hire_month, hire_day)
        
        # Убеждаемся, что дата приема не превышает текущую дату
        if hire_date > today:
            hire_date = today
        
        # 20% сотрудников уволены
        dismiss_date = None
        if random.random() < 0.2:
            dismiss_year = random.randint(hire_year, max_year)
            if dismiss_year == hire_year:
                # Если увольнение в том же году, месяц должен быть после месяца приема
                if hire_month < 12:
                    max_dismiss_month = today.month if dismiss_year == max_year else 12
                    dismiss_month = random.randint(hire_month + 1, max_dismiss_month)
                    if dismiss_month == today.month and dismiss_year == max_year:
                        dismiss_day = random.randint(1, min(today.day, 28))
                    else:
                        dismiss_day = random.randint(1, 28)
                else:
                    # Если принят в декабре, увольнение в следующем году
                    dismiss_year = min(dismiss_year + 1, max_year)
                    if dismiss_year == max_year:
                        dismiss_month = random.randint(1, today.month)
                        if dismiss_month == today.month:
                            dismiss_day = random.randint(1, min(today.day, 28))
                        else:
                            dismiss_day = random.randint(1, 28)
                    else:
                        dismiss_month = random.randint(1, 12)
                        dismiss_day = random.randint(1, 28)
            else:
                if dismiss_year == max_year:
                    dismiss_month = random.randint(1, today.month)
                    if dismiss_month == today.month:
                        dismiss_day = random.randint(1, min(today.day, 28))
                    else:
                        dismiss_day = random.randint(1, 28)
                else:
                    dismiss_month = random.randint(1, 12)
                    dismiss_day = random.randint(1, 28)
            dismiss_date = date(dismiss_year, dismiss_month, dismiss_day)
            
            # Убеждаемся, что дата увольнения не превышает текущую дату
            if dismiss_date > today:
                dismiss_date = today
            
            # Убеждаемся, что дата увольнения не раньше даты приема
            if dismiss_date < hire_date:
                dismiss_date = None
        
        # Генерация логина (фамилия + первая буква имени + ID)
        last_name_lower = last_name.lower()
        first_letter = first_name[0].lower()
        login = f"{last_name_lower}{first_letter}{i}"
        
        # Генерация пароля (по умолчанию: "password123" для всех, но можно изменить)
        # Для удобства тестирования используем простой пароль
        default_password = "password123"
        hashed_password = generate_password_hash(default_password)
        
        employee = Employee(
            ID_Сотрудника=i,
            ФИО_Сотрудника=fio,
            Должность=position,
            Дата_Приёма=hire_date,
            Дата_Увольнения=dismiss_date,
            Телефон_Сотрудника=phone,
            Логин=login,
            Пароль=hashed_password
        )
        employees.append(employee)
    
    async with async_session_maker() as session:
        session.add_all(employees)
        await session.commit()
    print(f"[OK] Создано {len(employees)} сотрудников")

async def create_admin_account():
    """Создание единственного администраторского аккаунта"""
    print("Создание администраторского аккаунта...")
    
    # Проверяем, есть ли уже администратор
    async with async_session_maker() as session:
        stmt = select(Employee).where(Employee.Должность == 'Администратор')
        result = await session.execute(stmt)
        existing_admin = result.scalar_one_or_none()
        if existing_admin:
            print("[OK] Администраторский аккаунт уже существует")
            return
        
        # Получаем следующий ID (после всех сотрудников)
        max_id_result = await session.execute(select(func.max(Employee.ID_Сотрудника)))
        max_id = max_id_result.scalar() or 0
        next_id = max_id + 1
        
        # Создаем администратора
        admin_password = "admin123"  # Пароль по умолчанию для админа
        hashed_password = generate_password_hash(admin_password)
        
        admin = Employee(
            ID_Сотрудника=next_id,
            ФИО_Сотрудника="Администратор Системы",
            Должность="Администратор",
            Дата_Приёма=date(2020, 1, 1),
            Дата_Увольнения=None,
            Телефон_Сотрудника="79999999999",
            Логин="admin",
            Пароль=hashed_password
        )
        
        session.add(admin)
        await session.commit()
        print(f"[OK] Создан администраторский аккаунт:")
        print(f"     Логин: admin")
        print(f"     Пароль: admin123")

async def generate_loans(count=250):
    """Генерация займов"""
    print(f"Генерация {count} займов...")
    
    async with async_session_maker() as session:
        clients_result = await session.execute(select(Client))
        clients = clients_result.scalars().all()
        
        employees_result = await session.execute(select(Employee).where(Employee.Дата_Увольнения == None))
        employees = employees_result.scalars().all()
        
        interest_rates_result = await session.execute(select(InterestRate))
        interest_rates = interest_rates_result.scalars().all()
        
        if not clients or not employees or not interest_rates:
            print("[ERROR] Недостаточно данных для создания займов")
            return
        
        today = date.today()
        # Создаем займы с датами от 2 лет назад до текущей даты
        # Это обеспечит наличие активных, просроченных и выплаченных займов
        start_date = date(today.year - 2, today.month, today.day)
        end_date = today  # Не превышаем текущую дату
        date_range = (end_date - start_date).days
        
        # Статистика для вывода
        status_counts = {'Активен': 0, 'Просрочен': 0, 'Выплачен': 0}
        
        loans = []
        for i in range(1, count + 1):
            loan_date = start_date + timedelta(days=random.randint(0, date_range))
            # Убеждаемся, что дата займа не превышает текущую дату
            if loan_date > today:
                loan_date = today
            
            client = random.choice(clients)
            employee = random.choice(employees)
            
            # Выбираем случайный процент по займу
            interest_rate = random.choice(interest_rates)
            
            # Вычисляем дату окончания займа
            months = float(interest_rate.Срок_займа)
            end_date_loan = loan_date + relativedelta(months=int(months))
            
            # Определяем статус на основе текущей даты и даты окончания займа
            rand = random.random()
            
            if end_date_loan < today:
                # Займ уже закончился
                if rand < 0.5:  # 50% из закончившихся - выплачены
                    status = 'Выплачен'
                else:  # 50% - просрочены
                    status = 'Просрочен'
            else:
                # Займ еще не закончился
                if rand < 0.2:  # 20% из активных могут быть выплачены досрочно
                    status = 'Выплачен'
                else:  # 80% - активны
                    status = 'Активен'
            
            status_counts[status] += 1
            
            # Товар - выбираем категорию и товар из этой категории
            category = random.choice(list(GOODS_DATA.keys()))
            goods_name, min_price, max_price = random.choice(GOODS_DATA[category])
            physical_condition = random.choice(PHYSICAL_CONDITIONS)
            
            # Размер займа зависит от товара и его состояния
            # Займ обычно составляет 50-70% от стоимости товара
            base_price = random.randint(min_price, max_price)
            # Учитываем физическое состояние при расчете займа
            condition_multiplier = {
                'Отличное': 0.7,
                'Хорошее': 0.6,
                'Удовлетворительное': 0.5,
                'Плохое': 0.4,
                'Ужасное': 0.3
            }
            loan_amount = Decimal(str(int(base_price * condition_multiplier[physical_condition])))
            # Ограничиваем максимальный размер займа до 999999.9999 (максимум для DECIMAL(10,4))
            max_loan = Decimal('999999.9999')
            if loan_amount > max_loan:
                loan_amount = max_loan
            
            loan = Loan(
                Код_займа=i,
                Дата_займа=loan_date,
                Клиент=client.ID_Клиента,
                Размер_займа=loan_amount,
                Процент_по_займу=interest_rate.Индекс_процента,
                Срок_займа=interest_rate.Срок_займа,
                Статус_займа=status,
                Состояние_товара=interest_rate.Состояние_товара,  # Процент из InterestRate
                Артикул_товара=i,
                Наименование_товара=goods_name,
                Категория_товара=category,
                Физическое_состояние=physical_condition,
                Исполнитель=employee.ID_Сотрудника
            )
            loans.append(loan)
        
        session.add_all(loans)
        await session.commit()
    print(f"[OK] Создано {len(loans)} займов")
    print(f"  Статистика по статусам:")
    print(f"    - Активных: {status_counts['Активен']}")
    print(f"    - Просроченных: {status_counts['Просрочен']}")
    print(f"    - Выплаченных: {status_counts['Выплачен']}")

async def generate_unclaimed_items():
    """Генерация невостребованных товаров из просроченных займов"""
    print("Генерация невостребованных товаров...")
    
    async with async_session_maker() as session:
        overdue_stmt = select(Loan).where(Loan.Статус_займа == 'Просрочен')
        overdue_result = await session.execute(overdue_stmt)
        overdue_loans = overdue_result.scalars().all()
        
        if not overdue_loans:
            print("  Нет просроченных займов для создания невостребованных товаров")
            return 0
        
        # Получаем список займов, для которых уже есть невостребованные товары
        existing_stmt = select(UnclaimedItem.Займ)
        existing_result = await session.execute(existing_stmt)
        existing_loan_ids = {item[0] for item in existing_result.all()}
        
        items = []
        # Находим максимальный артикул
        max_article_result = await session.execute(select(func.max(UnclaimedItem.Артикул)))
        max_article = max_article_result.scalar() or 0
        article = max_article + 1
    
    # ВСЕ просроченные займы, для которых еще нет невостребованных товаров, должны их получить
    for loan in overdue_loans:
        # Пропускаем займы, для которых уже есть невостребованный товар
        if loan.Код_займа in existing_loan_ids:
            continue
        
        # Оценочная стоимость должна быть разумной
        # Обычно она равна или немного больше размера займа (так как займ = 50-70% от стоимости)
        # Оценочная стоимость = размер займа / 0.6 (примерно, с учетом износа)
        estimated_value = loan.Размер_займа / Decimal('0.6')
        # Добавляем небольшой разброс ±10%
        estimated_value = estimated_value * Decimal(str(random.uniform(0.9, 1.1)))
        # Округляем до 4 знаков после запятой и ограничиваем максимумом
        estimated_value = estimated_value.quantize(Decimal('0.0001'))
        max_value = Decimal('999999.9999')
        if estimated_value > max_value:
            estimated_value = max_value
        
        item = UnclaimedItem(
            Артикул=article,
            Займ=loan.Код_займа,
            Оценочная_стоимость=estimated_value
        )
        items.append(item)
        article += 1
    
        if items:
            session.add_all(items)
            await session.commit()
            print(f"[OK] Создано {len(items)} невостребованных товаров")
        else:
            print("  Все просроченные займы уже имеют невостребованные товары")
    
    return len(items)

async def generate_sales(count=200):
    """Генерация продаж"""
    print(f"Генерация продаж...")
    
    async with async_session_maker() as session:
        unclaimed_result = await session.execute(select(UnclaimedItem))
        unclaimed_items = unclaimed_result.scalars().all()
        
        sales_employees_stmt = select(Employee).where(
            Employee.Должность == 'Менеджер по продажам',
            Employee.Дата_Увольнения == None
        )
        sales_employees_result = await session.execute(sales_employees_stmt)
        sales_employees = sales_employees_result.scalars().all()
        
        if not unclaimed_items:
            print("  Нет невостребованных товаров для продажи")
            return
        
        if not sales_employees:
            print("  Нет менеджеров по продажам")
            return
        
        sales = []
        today = date.today()
        start_date = date(2020, 1, 1)
        end_date = today  # Не превышаем текущую дату
        date_range = (end_date - start_date).days
        
        # Продаем только часть товаров (50-70%), чтобы остались непроданные
        # Это более реалистично - не все товары продаются сразу
        sell_percentage = random.uniform(0.5, 0.7)
        items_to_sell_count = int(len(unclaimed_items) * sell_percentage)
        items_to_sell = random.sample(unclaimed_items, items_to_sell_count)
        
        # Находим максимальный код продажи
        max_code_result = await session.execute(select(func.max(Sale.Код_продажи)))
        max_code = max_code_result.scalar() or 0
        
        for i, item in enumerate(items_to_sell, 1):
            # Дата продажи после даты займа
            loan = await session.get(Loan, item.Займ)
            if loan:
                loan_date = loan.Дата_займа
                # Дата продажи должна быть после даты займа, но не позже текущей даты
                max_days_after_loan = (today - loan_date).days
                if max_days_after_loan > 30:
                    days_after_loan = random.randint(30, min(365, max_days_after_loan))
                    sale_date = loan_date + timedelta(days=days_after_loan)
                elif max_days_after_loan > 0:
                    sale_date = loan_date + timedelta(days=random.randint(1, max_days_after_loan))
                else:
                    # Если займ в будущем, пропускаем этот товар
                    continue
                
                # Убеждаемся, что дата продажи не превышает текущую дату
                if sale_date > today:
                    sale_date = today
            else:
                sale_date = start_date + timedelta(days=random.randint(0, date_range))
            
            seller = random.choice(sales_employees)
            
            sale = Sale(
                Код_продажи=max_code + i,
                Дата_продажи=sale_date,
                Артикул_проданного_товара=item.Артикул,
                Продавец=seller.ID_Сотрудника
            )
            sales.append(sale)
        
        if sales:
            session.add_all(sales)
            await session.commit()
            print(f"[OK] Создано {len(sales)} продаж из {len(unclaimed_items)} невостребованных товаров")
            print(f"  Осталось непроданных товаров: {len(unclaimed_items) - len(sales)}")
        else:
            print("  Не создано продаж")

async def main():
    """Основная функция"""
    print("=" * 60)
    print("Инициализация базы данных CRM-системы ломбарда")
    print("=" * 60)
    
    try:
        # Создание таблиц
        await create_tables()
        
        # Установка SQL триггеров, функций и процедур
        await install_sql_scripts()
        
        # Генерация данных
        print("\n" + "=" * 60)
        print("Заполнение базы данных тестовыми данными")
        print("=" * 60)
        
        # Условно-постоянные данные (40-50 записей)
        await generate_interest_rates()  # 25 записей
        await generate_clients(50)  # 50 клиентов
        await generate_employees(45)  # 45 сотрудников
        await create_admin_account()  # Создание единственного администратора
        
        # Оперативно обновляющиеся данные (200-250 записей)
        await generate_loans(250)  # 250 займов
        unclaimed_count = await generate_unclaimed_items()  # ~60% от просроченных
        await generate_sales(200)  # 200 продаж
        
        print("\n" + "=" * 60)
        print("[OK] База данных успешно инициализирована!")
        print("=" * 60)
        print("\nСтатистика:")
        
        async with async_session_maker() as session:
            clients_count = (await session.execute(select(func.count(Client.ID_Клиента)))).scalar()
            employees_count = (await session.execute(select(func.count(Employee.ID_Сотрудника)))).scalar()
            rates_count = (await session.execute(select(func.count(InterestRate.Индекс_процента)))).scalar()
            loans_count = (await session.execute(select(func.count(Loan.Код_займа)))).scalar()
            unclaimed_count = (await session.execute(select(func.count(UnclaimedItem.Артикул)))).scalar()
            sales_count = (await session.execute(select(func.count(Sale.Код_продажи)))).scalar()
        
        print(f"  - Клиентов: {clients_count}")
        print(f"  - Сотрудников: {employees_count}")
        print(f"  - Процентов по займу: {rates_count}")
        print(f"  - Займов: {loans_count}")
        print(f"  - Невостребованных товаров: {unclaimed_count}")
        print(f"  - Продаж: {sales_count}")
        
    except Exception as e:
        print(f"\n[ERROR] Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    exit(asyncio.run(main()))
