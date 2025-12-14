from quart import Quart, render_template, request, jsonify, redirect, url_for, flash, session
from quart_auth import QuartAuth, AuthUser, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime, timedelta, date
from decimal import Decimal
from functools import wraps
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
from sqlalchemy import cast, String, or_, extract, select, func, distinct, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DBAPIError, IntegrityError
from models import Base, Client, Loan, UnclaimedItem, Sale, InterestRate, Employee, async_session_maker, engine
from config import config
import re

load_dotenv()

# Вспомогательная функция для извлечения понятного сообщения об ошибке из исключений БД
def extract_db_error_message(exception: Exception) -> str:
    """
    Извлекает понятное сообщение об ошибке из исключения БД.
    Обрабатывает ошибки от триггеров, ограничений и других объектов БД.
    """
    error_str = str(exception)
    
    # Если это DBAPIError, пытаемся извлечь оригинальное сообщение
    if hasattr(exception, 'orig'):
        orig_error = exception.orig
        if hasattr(orig_error, 'message'):
            error_str = str(orig_error.message)
        else:
            error_str = str(orig_error)
    
    # Если это ошибка от триггера PostgreSQL (RAISE EXCEPTION)
    # Формат: "Клиент с таким телефоном уже существует: 79795153805"
    # Или: "asyncpg.exceptions.RaiseError: Клиент с таким телефоном уже существует: 79795153805"
    if 'RaiseError' in error_str or 'RAISE' in error_str.upper():
        # Ищем русский текст в сообщении (обычно это сообщение от триггера)
        russian_match = re.search(r'([А-Яа-я][^:]+(?::\s*\d+)?)', error_str)
        if russian_match:
            return russian_match.group(1).strip()
        # Или извлекаем сообщение после последнего двоеточия
        parts = error_str.split(':')
        if len(parts) > 1:
            message = parts[-1].strip()
            # Убираем технические детали в скобках
            message = re.sub(r'\s*\([^)]+\)\s*$', '', message)
            return message
    
    # Ошибки уникальности (UNIQUE constraint)
    if 'unique' in error_str.lower() or 'уникаль' in error_str.lower():
        if 'телефон' in error_str.lower() or 'phone' in error_str.lower() or 'Телефон' in error_str:
            return 'Клиент или сотрудник с таким телефоном уже существует'
        if 'логин' in error_str.lower() or 'login' in error_str.lower() or 'Логин' in error_str:
            return 'Сотрудник с таким логином уже существует'
        return 'Нарушение уникальности данных'
    
    # Ошибки внешнего ключа (FOREIGN KEY constraint)
    if 'foreign key' in error_str.lower() or 'внешний ключ' in error_str.lower():
        return 'Невозможно выполнить операцию: связанные данные не найдены'
    
    # Ошибки проверки (CHECK constraint)
    if 'check' in error_str.lower() or 'проверка' in error_str.lower():
        return 'Данные не прошли проверку ограничений'
    
    # Ошибки NOT NULL
    if 'not null' in error_str.lower() or 'null value' in error_str.lower():
        return 'Не все обязательные поля заполнены'
    
    # Общие ошибки целостности данных
    if 'integrity' in error_str.lower():
        return 'Нарушение целостности данных'
    
    # Если сообщение уже на русском и понятное, возвращаем его
    if any(keyword in error_str for keyword in ['уже существует', 'не может', 'нельзя', 'должен', 'не найден', 'нельзя уволить']):
        # Извлекаем основное сообщение (русский текст)
        russian_match = re.search(r'([А-Яа-я][^:\.]+(?:\.|$))', error_str)
        if russian_match:
            return russian_match.group(1).strip()
        # Альтернативный вариант - берем текст после последнего двоеточия
        parts = error_str.split(':')
        if len(parts) > 1:
            message = parts[-1].strip()
            # Убираем технические детали
            message = re.sub(r'\s*\([^)]+\)\s*$', '', message)
            if any(c.isalpha() for c in message):  # Проверяем, что есть буквы
                return message
    
    # По умолчанию возвращаем общее сообщение
    return 'Произошла ошибка при выполнении операции с базой данных'

app = Quart(__name__)
# Использование конфигурационного файла
config_name = os.getenv('FLASK_ENV', 'development')
config_obj = config[config_name]()
app.config['SECRET_KEY'] = config_obj.SECRET_KEY

# Создаем класс пользователя для quart-auth
class User(AuthUser):
    def __init__(self, auth_id: str, employee: Employee = None):
        super().__init__(auth_id)
        self.employee = employee
    
    @property
    def Должность(self):
        if self.employee:
            return self.employee.Должность
        return None
    
    @property
    def ФИО_Сотрудника(self):
        if self.employee:
            return self.employee.ФИО_Сотрудника
        return None
    
    def is_active(self):
        if self.employee:
            return self.employee.is_active()
        return False

# Настройка Quart-Auth
# Кеш для пользователей (временное решение для resolve_user)
_user_cache = {}

# Создаем синхронный движок для resolve_user (если psycopg2 установлен)
_sync_engine = None
_SyncSession = None

try:
    # Пытаемся создать синхронный движок для загрузки пользователя
    config_name = os.getenv('FLASK_ENV', 'development')
    config_obj = config[config_name]()
    db_uri = config_obj.DATABASE_URI
    # Заменяем asyncpg на psycopg2 для синхронного доступа
    if db_uri.startswith('postgresql+asyncpg://'):
        db_uri = db_uri.replace('postgresql+asyncpg://', 'postgresql://', 1)
    elif db_uri.startswith('postgresql://'):
        pass  # Уже правильный формат
    else:
        # Формируем из параметров
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'Meow')
        db_name = os.getenv('DB_NAME', 'VorobievUD')
        db_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    _sync_engine = create_engine(db_uri, pool_pre_ping=True)
    _SyncSession = sessionmaker(bind=_sync_engine)
except Exception:
    # Если не удалось создать синхронный движок (psycopg2 не установлен), используем только кеш
    pass

class CustomQuartAuth(QuartAuth):
    def save_cookie(self, token: str) -> None:
        """Сохраняет токен в cookie"""
        try:
            super().save_cookie(token)
        except Exception:
            pass
    
    def load_token(self, token: str) -> str:
        """Загружает auth_id из токена"""
        try:
            result = super().load_token(token)
            if result:
                return str(result)
        except Exception:
            pass
        
        if isinstance(token, (str, int)):
            token_str = str(token)
            if token_str.isdigit():
                return token_str
        
        return None
    
    def resolve_user(self) -> AuthUser:
        try:
            if self.mode == "cookie":
                token = self.load_cookie()
            else:
                token = self.load_bearer()
            
            if not token:
                return User(None)
            
            auth_id = self.load_token(token)
            
            if not auth_id:
                if isinstance(token, (str, int)):
                    token_str = str(token)
                    if token_str.isdigit():
                        auth_id = token_str
                    else:
                        return User(None)
                else:
                    return User(None)
            
            if auth_id:
                auth_id_str = str(auth_id)
                
                if auth_id_str in _user_cache:
                    cached_data = _user_cache[auth_id_str]
                    if cached_data:
                        return User(auth_id_str, cached_data)
                
                if _SyncSession is not None:
                    try:
                        with _SyncSession() as sync_session:
                            employee = sync_session.get(Employee, int(auth_id_str))
                            if employee and employee.is_active():
                                _user_cache[auth_id_str] = employee
                                return User(auth_id_str, employee)
                    except (ValueError, TypeError):
                        pass
                    except Exception:
                        pass
                
                return User(None)
        except Exception:
            pass
        
        return User(None)

auth = CustomQuartAuth(app)
# Устанавливаем user_class для правильной работы с токенами
auth.user_class = User

# Настройка режима работы с токенами
auth.mode = "cookie"


# Определение прав доступа для должностей
ROLE_PERMISSIONS = {
    'Администратор': {
        'view_clients': True,
        'add_clients': True,
        'edit_clients': True,
        'delete_clients': True,
        'view_loans': True,
        'add_loans': True,
        'edit_loans': True,
        'pay_loans': True,
        'view_unclaimed': True,
        'add_unclaimed': True,
        'view_sales': True,
        'add_sales': True,
        'view_employees': True,
        'add_employees': True,
        'edit_employees': True,
        'dismiss_employees': True,
        'view_reports': True
    },
    'Менеджер-товаровед': {
        'view_clients': True,
        'add_clients': True,
        'edit_clients': True,
        'delete_clients': True,
        'view_loans': True,
        'add_loans': True,
        'edit_loans': True,
        'pay_loans': True,
        'view_unclaimed': True,
        'add_unclaimed': True,
        'view_sales': True,
        'add_sales': True,
        'view_employees': True,
        'add_employees': False,
        'edit_employees': False,
        'dismiss_employees': False,
        'view_reports': True
    },
    'Оценщик-товаровед': {
        'view_clients': True,
        'add_clients': True,
        'edit_clients': False,
        'delete_clients': False,
        'view_loans': True,
        'add_loans': True,
        'edit_loans': True,
        'pay_loans': True,
        'view_unclaimed': True,
        'add_unclaimed': True,
        'view_sales': True,
        'add_sales': False,
        'view_employees': True,
        'add_employees': False,
        'edit_employees': False,
        'dismiss_employees': False,
        'view_reports': False
    },
    'Менеджер по продажам': {
        'view_clients': True,
        'add_clients': False,
        'edit_clients': False,
        'delete_clients': False,
        'view_loans': True,
        'add_loans': False,
        'edit_loans': False,
        'pay_loans': False,
        'view_unclaimed': True,
        'add_unclaimed': False,
        'view_sales': True,
        'add_sales': True,
        'view_employees': True,
        'add_employees': False,
        'edit_employees': False,
        'dismiss_employees': False,
        'view_reports': True
    }
}

async def check_and_update_overdue_loans():
    """Проверяет и автоматически переводит просроченные займы в статус 'Просрочен'"""
    today = date.today()
    
    async with async_session_maker() as session:
        # Получаем все активные займы
        stmt = select(Loan).where(Loan.Статус_займа == 'Активен')
        result = await session.execute(stmt)
        active_loans = result.scalars().all()
        
        updated_count = 0
        for loan in active_loans:
            # Вычисляем дату окончания займа
            months = float(loan.Срок_займа)
            end_date = loan.Дата_займа + relativedelta(months=int(months))
            
            # Если сегодняшняя дата больше даты окончания, займ просрочен
            if today > end_date:
                loan.Статус_займа = 'Просрочен'
                updated_count += 1
        
        if updated_count > 0:
            await session.commit()
        
        return updated_count

def permission_required(permission):
    """Декоратор для проверки прав доступа"""
    def decorator(f):
        @wraps(f)
        @login_required
        async def decorated_function(*args, **kwargs):
            if not current_user.is_active():
                await flash('Ваш аккаунт неактивен', 'error')
                auth.logout_user()
                if current_user.auth_id:
                    _user_cache.pop(current_user.auth_id, None)
                return redirect(url_for('login'))
            
            user_role = current_user.Должность
            if user_role not in ROLE_PERMISSIONS:
                await flash('У вас нет доступа к этой функции', 'error')
                return redirect(url_for('index'))
            
            if not ROLE_PERMISSIONS[user_role].get(permission, False):
                await flash('У вас нет прав для выполнения этого действия', 'error')
                return redirect(url_for('index'))
            
            return await f(*args, **kwargs)
        return decorated_function
    return decorator

def has_permission(permission: str) -> bool:
    """Проверяет, есть ли у текущего пользователя указанное право доступа.
    Используется в шаблонах для условного отображения элементов."""
    try:
        if not hasattr(current_user, 'is_authenticated') or not current_user.is_authenticated:
            return False
        
        if not current_user.is_active():
            return False
        
        user_role = current_user.Должность
        if user_role not in ROLE_PERMISSIONS:
            return False
        
        return ROLE_PERMISSIONS[user_role].get(permission, False)
    except Exception:
        return False

# Делаем функцию доступной в шаблонах
@app.template_global()
def has_permission_global(permission: str) -> bool:
    """Глобальная функция для шаблонов - проверка прав доступа"""
    return has_permission(permission)

# ========== АВТОРИЗАЦИЯ ==========
@app.route('/login', methods=['GET', 'POST'])
async def login():
    # Очищаем флаг приветствия и flash сообщения при входе на страницу логина
    # Используем session из quart (импортирован в начале файла)
    from quart import session as quart_session
    quart_session.pop('welcome_shown', None)
    # Очищаем все flash сообщения при входе на страницу логина
    # (они могут остаться от предыдущих запросов)
    
    if request.method == 'POST':
        form = await request.form
        login_name = form.get('login')
        password = form.get('password')
        
        if not login_name or not password:
            await flash('Пожалуйста, введите логин и пароль', 'error')
            return await render_template('login.html')
        
        async with async_session_maker() as db_session:
            stmt = select(Employee).where(Employee.Логин == login_name)
            result = await db_session.execute(stmt)
            employee = result.scalar_one_or_none()
            
            if not employee:
                await flash('Неверный логин или пароль', 'error')
                return await render_template('login.html')
            
            if not employee.Логин:
                await flash('У этого сотрудника не установлен логин. Обратитесь к администратору.', 'error')
                return await render_template('login.html')
            
            if not employee.Пароль:
                await flash('У этого сотрудника не установлен пароль. Обратитесь к администратору.', 'error')
                return await render_template('login.html')
            
            password_match = check_password_hash(employee.Пароль, password)
            if not password_match:
                await flash('Неверный логин или пароль', 'error')
                return await render_template('login.html')
            
            is_active = employee.is_active()
            if not is_active:
                await flash('Ваш аккаунт неактивен (уволен)', 'error')
                return await render_template('login.html')
            
            auth_id = str(employee.ID_Сотрудника)
            _user_cache[auth_id] = employee
            
            user = User(auth_id, employee)
            auth.login_user(user)
            
            try:
                token = auth.dump_token(user.auth_id)
                next_page = request.args.get('next')
                response = redirect(next_page) if next_page else redirect(url_for('index'))
                
                cookie_name = getattr(auth, 'cookie_name', 'QUART_AUTH')
                response.set_cookie(
                    cookie_name,
                    token,
                    max_age=60 * 60 * 24 * 7,
                    httponly=True,
                    samesite='Lax'
                )
                return response
            except Exception:
                next_page = request.args.get('next')
                return redirect(next_page) if next_page else redirect(url_for('index'))
    
    return await render_template('login.html')

@app.route('/logout')
@login_required
async def logout():
    auth_id = current_user.auth_id if current_user.auth_id else None
    auth.logout_user()
    # Очищаем кеш при выходе
    if auth_id:
        _user_cache.pop(auth_id, None)
    await flash('Вы вышли из системы', 'info')
    return redirect(url_for('login'))

# Главная страница
@app.route('/')
@login_required
async def index():
    # Показываем приветственное сообщение только при первом входе
    # Проверяем, есть ли в сессии флаг первого входа
    from quart import session as quart_session
    if not quart_session.get('welcome_shown'):
        await flash(f'Добро пожаловать, {current_user.ФИО_Сотрудника}!', 'success')
        quart_session['welcome_shown'] = True
    
    # Автоматически проверяем и обновляем просроченные займы
    await check_and_update_overdue_loans()
    
    async with async_session_maker() as session:
        stats = {
            'total_clients': (await session.execute(select(func.count(Client.ID_Клиента)))).scalar(),
            'active_loans': (await session.execute(select(func.count(Loan.Код_займа)).where(Loan.Статус_займа != 'Выплачен'))).scalar(),
            'overdue_loans': (await session.execute(select(func.count(Loan.Код_займа)).where(Loan.Статус_займа == 'Просрочен'))).scalar(),
            'total_sales': (await session.execute(select(func.count(Sale.Код_продажи)))).scalar(),
            'total_employees': (await session.execute(select(func.count(Employee.ID_Сотрудника)).where(Employee.Дата_Увольнения == None))).scalar()
        }
    return await render_template('index.html', stats=stats)

# ========== КЛИЕНТЫ ==========
@app.route('/clients')
@login_required
@permission_required('view_clients')
async def clients():
    # Поиск
    search = request.args.get('search', '')
    # Сортировка
    sort_by = request.args.get('sort', 'ID_Клиента')
    sort_order = request.args.get('order', 'asc')
    
    async with async_session_maker() as session:
        stmt = select(Client)
        
        # Применяем поиск
        if search:
            # Проверяем, является ли поиск числом
            is_numeric = False
            search_int = None
            try:
                search_int = int(search)
                is_numeric = True
            except ValueError:
                pass
            
            search_conditions = []
            
            # Поиск по числовым полям
            if is_numeric:
                search_conditions.append(Client.ID_Клиента == search_int)
            
            # Поиск по числовым полям как строка
            search_conditions.append(cast(Client.ID_Клиента, String).ilike(f'%{search}%'))
            
            # Поиск по текстовым полям
            search_conditions.append(Client.ФИО.ilike(f'%{search}%'))
            search_conditions.append(Client.Телефон.ilike(f'%{search}%'))
            
            stmt = stmt.where(or_(*search_conditions))
        
        # Применяем сортировку
        if sort_by == 'ФИО':
            order_col = Client.ФИО
        elif sort_by == 'Телефон':
            order_col = Client.Телефон
        else:
            order_col = Client.ID_Клиента
        
        if sort_order == 'desc':
            stmt = stmt.order_by(order_col.desc())
        else:
            stmt = stmt.order_by(order_col.asc())
        
        result = await session.execute(stmt)
        clients_list = result.scalars().all()
    
    return await render_template('clients.html', clients=clients_list, search=search, sort=sort_by, order=sort_order)

@app.route('/clients/add', methods=['GET', 'POST'])
@login_required
@permission_required('add_clients')
async def add_client():
    if request.method == 'POST':
        try:
            form = await request.form
            async with async_session_maker() as session:
                # Получаем следующий ID клиента
                max_id_result = await session.execute(select(func.max(Client.ID_Клиента)))
                max_id = max_id_result.scalar() or 0
                next_id = max_id + 1
                
                client = Client(
                    ID_Клиента=next_id,
                    ФИО=form['fio'],
                    Телефон=form['phone']
                )
                session.add(client)
                await session.commit()
                await flash('Клиент успешно добавлен', 'success')
                return redirect(url_for('clients'))
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при добавлении клиента: {error_message}', 'error')
            return redirect(url_for('add_client'))
    return await render_template('add_client.html')

@app.route('/clients/<int:id>/edit', methods=['GET', 'POST'])
@login_required
@permission_required('edit_clients')
async def edit_client(id):
    async with async_session_maker() as session:
        client = await session.get(Client, id)
        if not client:
            await flash('Клиент не найден', 'error')
            return redirect(url_for('clients'))
        
        if request.method == 'POST':
            try:
                form = await request.form
                client.ФИО = form['fio']
                client.Телефон = form['phone']
                await session.commit()
                await flash('Клиент успешно обновлен', 'success')
                return redirect(url_for('clients'))
            except Exception as e:
                await session.rollback()
                error_message = extract_db_error_message(e)
                # Форматируем сообщение об ошибке в одну строку
                full_error_message = f'Ошибка при обновлении клиента: {error_message}'
                # Используем flash для toast-уведомления в правом верхнем углу
                await flash(full_error_message, 'error')
                # Загружаем объект заново в новой сессии для рендеринга
                async with async_session_maker() as new_session:
                    client = await new_session.get(Client, id)
                    return await render_template('edit_client.html', client=client)
        
        # GET запрос - объект уже загружен в текущей сессии
        return await render_template('edit_client.html', client=client)

@app.route('/clients/<int:id>/delete', methods=['POST'])
@login_required
@permission_required('delete_clients')
async def delete_client(id):
    async with async_session_maker() as session:
        client = await session.get(Client, id)
        if not client:
            await flash('Клиент не найден', 'error')
            return redirect(url_for('clients'))
        
        try:
            await session.delete(client)
            await session.commit()
            await flash('Клиент успешно удален', 'success')
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при удалении клиента: {error_message}', 'error')
    return redirect(url_for('clients'))

# ========== ЗАЙМЫ ==========
@app.route('/loans')
@login_required
@permission_required('view_loans')
async def loans():
    # Автоматически проверяем и обновляем просроченные займы
    await check_and_update_overdue_loans()
    
    # Фильтры
    status_filter = request.args.get('status', '')
    search = request.args.get('search', '')
    # Сортировка
    sort_by = request.args.get('sort', 'Код_займа')
    sort_order = request.args.get('order', 'desc')
    
    async with async_session_maker() as session:
        stmt = select(Loan)
        
        # Применяем фильтр по статусу
        if status_filter:
            stmt = stmt.where(Loan.Статус_займа == status_filter)
        
        # Применяем поиск
        if search:
            # Проверяем, является ли поиск числом
            is_numeric = False
            search_int = None
            try:
                search_int = int(search)
                is_numeric = True
            except ValueError:
                pass
            
            search_conditions = []
            
            # Поиск по числовым полям
            if is_numeric:
                search_conditions.append(Loan.Код_займа == search_int)
                search_conditions.append(Loan.Артикул_товара == search_int)
                search_conditions.append(Loan.Клиент == search_int)
            
            # Поиск по числовым полям как строка
            search_conditions.append(cast(Loan.Код_займа, String).ilike(f'%{search}%'))
            search_conditions.append(cast(Loan.Артикул_товара, String).ilike(f'%{search}%'))
            search_conditions.append(cast(Loan.Размер_займа, String).ilike(f'%{search}%'))
            search_conditions.append(cast(Loan.Срок_займа, String).ilike(f'%{search}%'))
            
            # Поиск по текстовым полям (требует join)
            stmt = stmt.join(Client, Loan.Клиент == Client.ID_Клиента)
            search_conditions.append(Client.ФИО.ilike(f'%{search}%'))
            search_conditions.append(Client.Телефон.ilike(f'%{search}%'))
            search_conditions.append(Loan.Наименование_товара.ilike(f'%{search}%'))
            search_conditions.append(Loan.Категория_товара.ilike(f'%{search}%'))
            search_conditions.append(Loan.Физическое_состояние.ilike(f'%{search}%'))
            search_conditions.append(Loan.Статус_займа.ilike(f'%{search}%'))
            
            stmt = stmt.where(or_(*search_conditions)).distinct()
        
        # Применяем сортировку
        if sort_by == 'Дата':
            order_col = Loan.Дата_займа
        elif sort_by == 'Размер':
            order_col = Loan.Размер_займа
        elif sort_by == 'Срок':
            order_col = Loan.Срок_займа
        elif sort_by == 'Состояние':
            order_col = Loan.Статус_займа
        else:
            order_col = Loan.Код_займа
        
        if sort_order == 'desc':
            stmt = stmt.order_by(order_col.desc())
        else:
            stmt = stmt.order_by(order_col.asc())
        
        result = await session.execute(stmt)
        loans_list = result.scalars().all()
        
        # Вычисляем даты окончания для всех займов
        today = date.today()
        loans_with_end_dates = []
        for loan in loans_list:
            months = float(loan.Срок_займа)
            end_date = loan.Дата_займа + relativedelta(months=int(months))
            loans_with_end_dates.append({
                'loan': loan,
                'end_date': end_date,
                'days_left': (end_date - today).days if loan.Статус_займа == 'Активен' else None
            })
        
        # Получаем уникальные статусы для фильтра
        status_stmt = select(distinct(Loan.Статус_займа))
        status_result = await session.execute(status_stmt)
        status_list = [s[0] for s in status_result.all()]
    
    return await render_template('loans.html', loans=loans_with_end_dates, status_filter=status_filter, 
                          search=search, sort=sort_by, order=sort_order, statuses=status_list, today=today)

@app.route('/api/loan-autocomplete', methods=['GET'])
@login_required
async def loan_autocomplete():
    """API endpoint для автозаполнения полей формы займа"""
    try:
        async with async_session_maker() as session:
            # Получаем уникальные коды займов с полными данными (артикул = код займа)
            stmt = select(
                Loan.Код_займа,
                Loan.Наименование_товара,
                Loan.Категория_товара,
                Loan.Физическое_состояние
            ).distinct().order_by(Loan.Код_займа)
            
            result = await session.execute(stmt)
            loans_data = result.all()
            
            # Формируем данные для автозаполнения
            # Используем код займа как ключ (так как артикул = код займа)
            loan_codes = {}
            names = set()
            categories = set()
            
            for loan in loans_data:
                loan_code = loan.Код_займа
                name = loan.Наименование_товара
                category = loan.Категория_товара
                condition = loan.Физическое_состояние
                
                # Сохраняем полные данные по коду займа (берем последний найденный)
                loan_codes[loan_code] = {
                    'name': name,
                    'category': category,
                    'condition': condition
                }
                
                names.add(name)
                categories.add(category)
            
            return jsonify({
                'loan_codes': {str(k): v for k, v in loan_codes.items()},
                'names': sorted(list(names)),
                'categories': sorted(list(categories))
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/loans/add', methods=['GET', 'POST'])
@login_required
@permission_required('add_loans')
async def add_loan():
    if request.method == 'POST':
        try:
            form = await request.form
            async with async_session_maker() as session:
                # Получаем процент по займу на основе состояния товара и срока
                condition_decimal = Decimal(form['condition'])
                term_decimal = Decimal(form['term'])
                
                stmt = select(InterestRate).where(
                    InterestRate.Состояние_товара == condition_decimal,
                    InterestRate.Срок_займа == term_decimal
                )
                result = await session.execute(stmt)
                interest_rate = result.scalar_one_or_none()
                
                # Если InterestRate не найден, создаем новый
                if not interest_rate:
                    # Получаем максимальный индекс процента
                    max_index_stmt = select(func.max(InterestRate.Индекс_процента))
                    max_index_result = await session.execute(max_index_stmt)
                    max_index = max_index_result.scalar() or 0
                    new_index = max_index + 1
                    
                    # Рассчитываем процент: состояние товара + срок займа
                    calculated_percentage = condition_decimal + term_decimal
                    
                    # Создаем новый InterestRate
                    interest_rate = InterestRate(
                        Индекс_процента=new_index,
                        Состояние_товара=condition_decimal,
                        Срок_займа=term_decimal,
                        Процент=calculated_percentage
                    )
                    session.add(interest_rate)
                    await session.flush()  # Сохраняем, чтобы получить ID
                
                # Получаем следующий код займа (так как Код_займа не является автоинкрементом)
                max_code_stmt = select(func.max(Loan.Код_займа))
                max_code_result = await session.execute(max_code_stmt)
                max_code = max_code_result.scalar() or 0
                next_loan_code = max_code + 1
                
                # Создаем займ с явным указанием Код_займа и артикула (равного коду займа)
                loan = Loan(
                    Код_займа=next_loan_code,  # Явно указываем код займа
                    Дата_займа=datetime.strptime(form['date'], '%Y-%m-%d').date(),
                    Клиент=int(form['client_id']),
                    Размер_займа=Decimal(form['amount']),
                    Процент_по_займу=interest_rate.Индекс_процента,
                    Срок_займа=term_decimal,
                    Статус_займа='Активен',  # Новый займ всегда активен
                    Состояние_товара=interest_rate.Состояние_товара,  # Процент из InterestRate
                    Артикул_товара=next_loan_code,  # Артикул равен коду займа
                    Наименование_товара=form['name'],
                    Категория_товара=form['category'],
                    Физическое_состояние=form['physical_condition'],
                    Исполнитель=int(form['employee_id'])
                )
                session.add(loan)
                await session.commit()
                await flash('Займ успешно добавлен', 'success')
                return redirect(url_for('loans'))
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при добавлении займа: {error_message}', 'error')
            return redirect(url_for('add_loan'))
    
    async with async_session_maker() as session:
        clients_result = await session.execute(select(Client))
        clients_list = clients_result.scalars().all()
        
        employees_result = await session.execute(select(Employee).where(Employee.Дата_Увольнения == None))
        employees_list = employees_result.scalars().all()
        
        interest_rates_result = await session.execute(select(InterestRate))
        interest_rates = interest_rates_result.scalars().all()
    
    return await render_template('add_loan.html', clients=clients_list, employees=employees_list, interest_rates=interest_rates)

@app.route('/loans/<int:id>')
@login_required
@permission_required('view_loans')
async def loan_detail(id):
    async with async_session_maker() as session:
        loan = await session.get(Loan, id)
        if not loan:
            await flash('Займ не найден', 'error')
            return redirect(url_for('loans'))
        
        # Вычисляем дату окончания займа
        months = float(loan.Срок_займа)
        end_date = loan.Дата_займа + relativedelta(months=int(months))
        today = date.today()
    
    return await render_template('loan_detail.html', loan=loan, end_date=end_date, today=today)

@app.route('/loans/<int:id>/pay', methods=['POST'])
@login_required
@permission_required('pay_loans')
async def pay_loan(id):
    async with async_session_maker() as session:
        loan = await session.get(Loan, id)
        if not loan:
            await flash('Займ не найден', 'error')
            return redirect(url_for('loans'))
        
        # Просроченные займы не могут быть отмечены как выплаченные
        if loan.Статус_займа == 'Просрочен':
            await flash('Товар переведен в невостребованные.', 'info')
            return redirect(url_for('loan_detail', id=id))
        
        # Только активные займы могут быть отмечены как выплаченные
        if loan.Статус_займа != 'Активен':
            await flash('Займ уже имеет статус "Выплачен"', 'error')
            return redirect(url_for('loan_detail', id=id))
        
        try:
            loan.Статус_займа = 'Выплачен'
            await session.commit()
            await flash('Займ отмечен как выплаченный', 'success')
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при обновлении займа: {error_message}', 'error')
    return redirect(url_for('loan_detail', id=id))

# ========== НЕВОСТРЕБОВАННЫЕ ТОВАРЫ ==========
@app.route('/unclaimed')
@login_required
@permission_required('view_unclaimed')
async def unclaimed_items():
    # Фильтры
    search = request.args.get('search', '')
    min_price = request.args.get('min_price', '')
    max_price = request.args.get('max_price', '')
    # Сортировка
    sort_by = request.args.get('sort', 'Артикул')
    sort_order = request.args.get('order', 'desc')
    
    async with async_session_maker() as session:
        # Применяем поиск
        if search:
            # Проверяем, является ли поиск числом
            is_numeric = False
            search_int = None
            try:
                search_int = int(search)
                is_numeric = True
            except ValueError:
                pass
            
            # Создаем список условий для поиска
            search_conditions = []
            
            # Поиск по полям самой таблицы UnclaimedItem (не требует join)
            if is_numeric:
                # Точное совпадение по числу
                search_conditions.append(UnclaimedItem.Артикул == search_int)
                search_conditions.append(UnclaimedItem.Займ == search_int)
            # Всегда добавляем поиск как строку для частичного совпадения
            search_conditions.append(cast(UnclaimedItem.Артикул, String).ilike(f'%{search}%'))
            search_conditions.append(cast(UnclaimedItem.Займ, String).ilike(f'%{search}%'))
            
            # Поиск по оценочной стоимости (всегда как строка)
            search_conditions.append(cast(UnclaimedItem.Оценочная_стоимость, String).ilike(f'%{search}%'))
            
            # Для поиска по связанным таблицам нужен join
            # Создаем базовый запрос с join
            stmt = select(UnclaimedItem).join(Loan, UnclaimedItem.Займ == Loan.Код_займа).join(Client, Loan.Клиент == Client.ID_Клиента)
            
            # Добавляем условия поиска по связанным таблицам
            search_conditions.append(Client.ФИО.ilike(f'%{search}%'))
            search_conditions.append(Loan.Наименование_товара.ilike(f'%{search}%'))
            search_conditions.append(Loan.Категория_товара.ilike(f'%{search}%'))
            
            # Применяем условия поиска (показываем все товары, включая проданные)
            stmt = stmt.where(or_(*search_conditions)).distinct()
        else:
            # Если поиска нет, показываем все товары
            stmt = select(UnclaimedItem)
        
        # Применяем фильтр по цене
        if min_price:
            try:
                stmt = stmt.where(UnclaimedItem.Оценочная_стоимость >= Decimal(min_price))
            except:
                pass
        if max_price:
            try:
                stmt = stmt.where(UnclaimedItem.Оценочная_стоимость <= Decimal(max_price))
            except:
                pass
        
        # Применяем сортировку
        if sort_by == 'Стоимость':
            order_col = UnclaimedItem.Оценочная_стоимость
        elif sort_by == 'Займ':
            order_col = UnclaimedItem.Займ
        else:
            order_col = UnclaimedItem.Артикул
        
        if sort_order == 'desc':
            stmt = stmt.order_by(order_col.desc())
        else:
            stmt = stmt.order_by(order_col.asc())
        
        result = await session.execute(stmt)
        items = result.scalars().all()
    
    return await render_template('unclaimed_items.html', items=items, search=search, 
                          min_price=min_price, max_price=max_price, sort=sort_by, order=sort_order)

@app.route('/unclaimed/add', methods=['GET', 'POST'])
@login_required
@permission_required('add_unclaimed')
async def add_unclaimed_item():
    if request.method == 'POST':
        try:
            form = await request.form
            loan_id = int(form['loan_id'])
            
            async with async_session_maker() as session:
                # Проверяем, что для этого займа еще нет невостребованного товара
                stmt = select(UnclaimedItem).where(UnclaimedItem.Займ == loan_id)
                result = await session.execute(stmt)
                existing_item = result.scalar_one_or_none()
                if existing_item:
                    await flash('Для этого займа уже создан невостребованный товар', 'error')
                    return redirect(url_for('add_unclaimed_item'))
                
                # Проверяем, что займ просрочен
                loan = await session.get(Loan, loan_id)
                if not loan:
                    await flash('Займ не найден', 'error')
                    return redirect(url_for('add_unclaimed_item'))
                if loan.Статус_займа != 'Просрочен':
                    await flash('Невостребованный товар можно создать только для просроченного займа', 'error')
                    return redirect(url_for('add_unclaimed_item'))
                
                # Артикул невостребованного товара равен коду займа (который также равен артикулу товара в займе)
                item = UnclaimedItem(
                    Артикул=loan.Код_займа,  # Артикул равен коду займа
                    Займ=loan_id,
                    Оценочная_стоимость=Decimal(form['estimated_value'])
                )
                session.add(item)
                await session.commit()
                await flash('Невостребованный товар успешно добавлен', 'success')
                return redirect(url_for('unclaimed_items'))
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при добавлении товара: {error_message}', 'error')
            return redirect(url_for('add_unclaimed_item'))
    
    async with async_session_maker() as session:
        # Показываем только просроченные займы, для которых еще нет невостребованных товаров
        overdue_stmt = select(Loan).where(Loan.Статус_займа == 'Просрочен')
        overdue_result = await session.execute(overdue_stmt)
        overdue_loans = overdue_result.scalars().all()
        
        existing_stmt = select(UnclaimedItem.Займ)
        existing_result = await session.execute(existing_stmt)
        existing_loan_ids = {item[0] for item in existing_result.all()}
        available_loans = [loan for loan in overdue_loans if loan.Код_займа not in existing_loan_ids]
    
    # Получаем loan_id из URL параметра, если есть
    preselected_loan_id = request.args.get('loan_id', type=int)
    
    return await render_template('add_unclaimed_item.html', loans=available_loans, preselected_loan_id=preselected_loan_id)

# ========== ПРОДАЖИ ==========
@app.route('/sales')
@login_required
@permission_required('view_sales')
async def sales():
    # Фильтры
    search = request.args.get('search', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    # Сортировка
    sort_by = request.args.get('sort', 'Код_продажи')
    sort_order = request.args.get('order', 'desc')
    
    async with async_session_maker() as session:
        stmt = select(Sale)
        
        # Применяем поиск
        if search:
            # Проверяем, является ли поиск числом
            is_numeric = False
            search_int = None
            try:
                search_int = int(search)
                is_numeric = True
            except ValueError:
                pass
            
            search_conditions = []
            
            # Поиск по числовым полям
            if is_numeric:
                search_conditions.append(Sale.Код_продажи == search_int)
                search_conditions.append(Sale.Артикул_проданного_товара == search_int)
            
            # Поиск по числовым полям как строка
            search_conditions.append(cast(Sale.Код_продажи, String).ilike(f'%{search}%'))
            search_conditions.append(cast(Sale.Артикул_проданного_товара, String).ilike(f'%{search}%'))
            
            # Поиск по связанным таблицам (требует join)
            stmt = stmt.join(UnclaimedItem, Sale.Артикул_проданного_товара == UnclaimedItem.Артикул)
            stmt = stmt.join(Loan, UnclaimedItem.Займ == Loan.Код_займа)
            stmt = stmt.join(Client, Loan.Клиент == Client.ID_Клиента)
            stmt = stmt.join(Employee, Sale.Продавец == Employee.ID_Сотрудника)
            
            # Поиск по числовым полям связанных таблиц
            if is_numeric:
                search_conditions.append(UnclaimedItem.Займ == search_int)
                search_conditions.append(Loan.Код_займа == search_int)
            
            search_conditions.append(cast(UnclaimedItem.Займ, String).ilike(f'%{search}%'))
            search_conditions.append(cast(UnclaimedItem.Оценочная_стоимость, String).ilike(f'%{search}%'))
            search_conditions.append(cast(Loan.Код_займа, String).ilike(f'%{search}%'))
            
            # Поиск по текстовым полям
            search_conditions.append(Client.ФИО.ilike(f'%{search}%'))
            search_conditions.append(Client.Телефон.ilike(f'%{search}%'))
            search_conditions.append(Loan.Наименование_товара.ilike(f'%{search}%'))
            search_conditions.append(Loan.Категория_товара.ilike(f'%{search}%'))
            search_conditions.append(Employee.ФИО_Сотрудника.ilike(f'%{search}%'))
            search_conditions.append(Employee.Должность.ilike(f'%{search}%'))
            
            stmt = stmt.where(or_(*search_conditions)).distinct()
        
        # Применяем фильтр по дате
        if date_from:
            try:
                stmt = stmt.where(Sale.Дата_продажи >= datetime.strptime(date_from, '%Y-%m-%d').date())
            except:
                pass
        if date_to:
            try:
                stmt = stmt.where(Sale.Дата_продажи <= datetime.strptime(date_to, '%Y-%m-%d').date())
            except:
                pass
        
        # Применяем сортировку
        if sort_by == 'Дата':
            order_col = Sale.Дата_продажи
        elif sort_by == 'Артикул':
            order_col = Sale.Артикул_проданного_товара
        else:
            order_col = Sale.Код_продажи
        
        if sort_order == 'desc':
            stmt = stmt.order_by(order_col.desc())
        else:
            stmt = stmt.order_by(order_col.asc())
        
        result = await session.execute(stmt)
        sales_list = result.scalars().all()
    
    return await render_template('sales.html', sales=sales_list, search=search, 
                          date_from=date_from, date_to=date_to, sort=sort_by, order=sort_order)

@app.route('/sales/add', methods=['GET', 'POST'])
@login_required
@permission_required('add_sales')
async def add_sale():
    if request.method == 'POST':
        try:
            form = await request.form
            article_id = int(form['article_id'])
            
            async with async_session_maker() as session:
                # Проверяем, не продан ли уже этот товар
                stmt = select(Sale).where(Sale.Артикул_проданного_товара == article_id)
                result = await session.execute(stmt)
                existing_sale = result.scalar_one_or_none()
                if existing_sale:
                    await flash('Этот товар уже был продан ранее', 'error')
                    return redirect(url_for('add_sale'))
                
                # Получаем следующий код продажи
                max_code_result = await session.execute(select(func.max(Sale.Код_продажи)))
                max_code = max_code_result.scalar() or 0
                next_code = max_code + 1
                
                sale = Sale(
                    Код_продажи=next_code,
                    Дата_продажи=datetime.strptime(form['date'], '%Y-%m-%d').date(),
                    Артикул_проданного_товара=article_id,
                    Продавец=int(form['seller_id'])
                )
                session.add(sale)
                await session.commit()
                await flash('Продажа успешно добавлена', 'success')
                return redirect(url_for('sales'))
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при добавлении продажи: {error_message}', 'error')
            return redirect(url_for('add_sale'))
    
    async with async_session_maker() as session:
        # Показываем только непроданные товары
        sold_stmt = select(Sale.Артикул_проданного_товара)
        sold_result = await session.execute(sold_stmt)
        sold_article_ids = [sale[0] for sale in sold_result.all()]
        
        if sold_article_ids:
            unclaimed_stmt = select(UnclaimedItem).where(~UnclaimedItem.Артикул.in_(sold_article_ids))
        else:
            unclaimed_stmt = select(UnclaimedItem)
        unclaimed_result = await session.execute(unclaimed_stmt)
        unclaimed_items = unclaimed_result.scalars().all()
        
        employees_stmt = select(Employee).where(
            Employee.Должность == 'Менеджер по продажам',
            Employee.Дата_Увольнения == None
        )
        employees_result = await session.execute(employees_stmt)
        employees = employees_result.scalars().all()
    
    return await render_template('add_sale.html', unclaimed_items=unclaimed_items, employees=employees)

# ========== СОТРУДНИКИ ==========
@app.route('/employees')
@login_required
@permission_required('view_employees')
async def employees():
    # Фильтры
    search = request.args.get('search', '')
    position_filter = request.args.get('position', '')
    status_filter = request.args.get('status', '')
    # Сортировка
    sort_by = request.args.get('sort', 'ID_Сотрудника')
    sort_order = request.args.get('order', 'asc')
    
    async with async_session_maker() as session:
        stmt = select(Employee)
        
        # Применяем поиск
        if search:
            # Проверяем, является ли поиск числом
            is_numeric = False
            search_int = None
            try:
                search_int = int(search)
                is_numeric = True
            except ValueError:
                pass
            
            search_conditions = []
            
            # Поиск по числовым полям
            if is_numeric:
                search_conditions.append(Employee.ID_Сотрудника == search_int)
            
            # Поиск по числовым полям как строка
            search_conditions.append(cast(Employee.ID_Сотрудника, String).ilike(f'%{search}%'))
            
            # Поиск по текстовым полям
            search_conditions.append(Employee.ФИО_Сотрудника.ilike(f'%{search}%'))
            search_conditions.append(Employee.Телефон_Сотрудника.ilike(f'%{search}%'))
            search_conditions.append(Employee.Должность.ilike(f'%{search}%'))
            
            stmt = stmt.where(or_(*search_conditions))
        
        # Применяем фильтр по должности
        if position_filter:
            stmt = stmt.where(Employee.Должность == position_filter)
        
        # Применяем фильтр по статусу
        if status_filter == 'active':
            stmt = stmt.where(Employee.Дата_Увольнения == None)
        elif status_filter == 'dismissed':
            stmt = stmt.where(Employee.Дата_Увольнения != None)
        
        # Применяем сортировку
        if sort_by == 'ФИО':
            order_col = Employee.ФИО_Сотрудника
        elif sort_by == 'Должность':
            order_col = Employee.Должность
        elif sort_by == 'Дата_приема':
            order_col = Employee.Дата_Приёма
        else:
            order_col = Employee.ID_Сотрудника
        
        if sort_order == 'desc':
            stmt = stmt.order_by(order_col.desc())
        else:
            stmt = stmt.order_by(order_col.asc())
        
        result = await session.execute(stmt)
        employees_list = result.scalars().all()
        
        # Получаем уникальные должности для фильтра
        positions_stmt = select(distinct(Employee.Должность))
        positions_result = await session.execute(positions_stmt)
        position_list = [p[0] for p in positions_result.all()]
    
    return await render_template('employees.html', employees=employees_list, search=search,
                          position_filter=position_filter, status_filter=status_filter,
                          sort=sort_by, order=sort_order, positions=position_list)

@app.route('/employees/add', methods=['GET', 'POST'])
@login_required
@permission_required('add_employees')
async def add_employee():
    if request.method == 'POST':
        try:
            form = await request.form
            async with async_session_maker() as session:
                # Проверяем, что логин уникален
                stmt = select(Employee).where(Employee.Логин == form['login'])
                result = await session.execute(stmt)
                existing_employee = result.scalar_one_or_none()
                if existing_employee:
                    await flash('Сотрудник с таким логином уже существует', 'error')
                    return redirect(url_for('add_employee'))
                
                # Проверяем, что не создается второй администратор
                if form['position'] == 'Администратор':
                    admin_stmt = select(Employee).where(Employee.Должность == 'Администратор')
                    admin_result = await session.execute(admin_stmt)
                    existing_admin = admin_result.scalar_one_or_none()
                    if existing_admin:
                        await flash('Администратор уже существует. Может быть только один администратор.', 'error')
                        return redirect(url_for('add_employee'))
                
                # Получаем следующий ID сотрудника
                max_id_result = await session.execute(select(func.max(Employee.ID_Сотрудника)))
                max_id = max_id_result.scalar() or 0
                next_id = max_id + 1
                
                # Хешируем пароль
                hashed_password = generate_password_hash(form['password'])
                
                employee = Employee(
                    ID_Сотрудника=next_id,
                    ФИО_Сотрудника=form['fio'],
                    Должность=form['position'],
                    Дата_Приёма=datetime.strptime(form['hire_date'], '%Y-%m-%d').date(),
                    Телефон_Сотрудника=form['phone'],
                    Логин=form['login'],
                    Пароль=hashed_password
                )
                session.add(employee)
                await session.commit()
                await flash('Сотрудник успешно добавлен', 'success')
                return redirect(url_for('employees'))
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при добавлении сотрудника: {error_message}', 'error')
            return redirect(url_for('add_employee'))
    return await render_template('add_employee.html')

@app.route('/employees/<int:id>/edit', methods=['GET', 'POST'])
@login_required
@permission_required('edit_employees')
async def edit_employee(id):
    async with async_session_maker() as session:
        employee = await session.get(Employee, id)
        if not employee:
            await flash('Сотрудник не найден', 'error')
            return redirect(url_for('employees'))
        
        if request.method == 'POST':
            try:
                form = await request.form
                
                # Проверяем, что логин уникален (если изменился)
                if form['login'] != employee.Логин:
                    stmt = select(Employee).where(Employee.Логин == form['login'])
                    result = await session.execute(stmt)
                    existing_employee = result.scalar_one_or_none()
                    if existing_employee:
                        await flash('Сотрудник с таким логином уже существует', 'error')
                        return redirect(url_for('edit_employee', id=id))
                
                # Проверяем, что не создается второй администратор (если меняется должность)
                if form['position'] == 'Администратор' and employee.Должность != 'Администратор':
                    admin_stmt = select(Employee).where(Employee.Должность == 'Администратор')
                    admin_result = await session.execute(admin_stmt)
                    existing_admin = admin_result.scalar_one_or_none()
                    if existing_admin:
                        await flash('Администратор уже существует. Может быть только один администратор.', 'error')
                        return redirect(url_for('edit_employee', id=id))
                
                # Обновляем данные
                employee.ФИО_Сотрудника = form['fio']
                employee.Должность = form['position']
                employee.Дата_Приёма = datetime.strptime(form['hire_date'], '%Y-%m-%d').date()
                employee.Телефон_Сотрудника = form['phone']
                employee.Логин = form['login']
                
                # Обновляем пароль только если он указан
                if form.get('password'):
                    employee.Пароль = generate_password_hash(form['password'])
                
                await session.commit()
                await flash('Сотрудник успешно обновлен', 'success')
                return redirect(url_for('employees'))
            except Exception as e:
                await session.rollback()
                error_message = extract_db_error_message(e)
                await flash(f'Ошибка при обновлении сотрудника: {error_message}', 'error')
                # Перезагружаем сотрудника для отображения формы
                await session.refresh(employee)
        
        return await render_template('edit_employee.html', employee=employee)

@app.route('/employees/<int:id>/dismiss', methods=['POST'])
@login_required
@permission_required('dismiss_employees')
async def dismiss_employee(id):
    async with async_session_maker() as session:
        employee = await session.get(Employee, id)
        if not employee:
            await flash('Сотрудник не найден', 'error')
            return redirect(url_for('employees'))
        
        # Администратора нельзя уволить
        if employee.Должность == 'Администратор':
            await flash('Администратора нельзя уволить', 'error')
            return redirect(url_for('employees'))
        
        try:
            employee.Дата_Увольнения = datetime.now().date()
            await session.commit()
            await flash('Сотрудник уволен', 'success')
        except Exception as e:
            await session.rollback()
            error_message = extract_db_error_message(e)
            await flash(f'Ошибка при увольнении сотрудника: {error_message}', 'error')
    return redirect(url_for('employees'))

# ========== ОТЧЕТЫ ==========
@app.route('/reports')
@login_required
@permission_required('view_reports')
async def reports():
    async with async_session_maker() as session:
        # Получаем все года, для которых есть займы или продажи
        loan_years_stmt = select(distinct(extract('year', Loan.Дата_займа).label('year')))
        loan_years_result = await session.execute(loan_years_stmt)
        loan_years = loan_years_result.all()
        
        sale_years_stmt = select(distinct(extract('year', Sale.Дата_продажи).label('year')))
        sale_years_result = await session.execute(sale_years_stmt)
        sale_years = sale_years_result.all()
        
        # Объединяем и получаем уникальные года
        all_years = set()
        for year_tuple in loan_years:
            if year_tuple[0]:
                all_years.add(int(year_tuple[0]))
        for year_tuple in sale_years:
            if year_tuple[0]:
                all_years.add(int(year_tuple[0]))
        
        # Сортируем года по убыванию
        available_years = sorted(all_years, reverse=True) if all_years else [datetime.now().year]
        
        # Для каждого года определяем кварталы с данными
        year_quarters = {}
        for year in available_years:
            quarters = set()
            
            # Проверяем займы
            for quarter in range(1, 5):
                if quarter == 1:
                    start_date = date(year, 1, 1)
                    end_date = date(year, 3, 31)
                elif quarter == 2:
                    start_date = date(year, 4, 1)
                    end_date = date(year, 6, 30)
                elif quarter == 3:
                    start_date = date(year, 7, 1)
                    end_date = date(year, 9, 30)
                else:
                    start_date = date(year, 10, 1)
                    end_date = date(year, 12, 31)
                
                # Проверяем, есть ли займы или продажи в этом квартале
                has_loans_stmt = select(Loan).where(
                    Loan.Дата_займа >= start_date,
                    Loan.Дата_займа <= end_date
                ).limit(1)
                has_loans_result = await session.execute(has_loans_stmt)
                has_loans = has_loans_result.scalar_one_or_none() is not None
                
                has_sales_stmt = select(Sale).where(
                    Sale.Дата_продажи >= start_date,
                    Sale.Дата_продажи <= end_date
                ).limit(1)
                has_sales_result = await session.execute(has_sales_stmt)
                has_sales = has_sales_result.scalar_one_or_none() is not None
                
                if has_loans or has_sales:
                    quarters.add(quarter)
            
            if quarters:
                year_quarters[year] = sorted(list(quarters))
    
    return await render_template('reports.html', available_years=available_years, year_quarters=year_quarters)

@app.route('/api/reports/quarterly')
async def quarterly_report():
    quarter = request.args.get('quarter', '1')
    year = request.args.get('year', datetime.now().year)
    
    # Определяем даты квартала
    if quarter == '1':
        start_date = datetime(int(year), 1, 1)
        end_date = datetime(int(year), 3, 31)
    elif quarter == '2':
        start_date = datetime(int(year), 4, 1)
        end_date = datetime(int(year), 6, 30)
    elif quarter == '3':
        start_date = datetime(int(year), 7, 1)
        end_date = datetime(int(year), 9, 30)
    else:
        start_date = datetime(int(year), 10, 1)
        end_date = datetime(int(year), 12, 31)
    
    async with async_session_maker() as session:
        loans_stmt = select(Loan).where(
            Loan.Дата_займа >= start_date.date(),
            Loan.Дата_займа <= end_date.date()
        )
        loans_result = await session.execute(loans_stmt)
        loans = loans_result.scalars().all()
        
        sales_stmt = select(Sale).where(
            Sale.Дата_продажи >= start_date.date(),
            Sale.Дата_продажи <= end_date.date()
        )
        sales_result = await session.execute(sales_stmt)
        sales = sales_result.scalars().all()
        
        # Расчет суммы продаж
        sales_amount = 0.0
        for sale in sales:
            item_stmt = select(UnclaimedItem).where(UnclaimedItem.Артикул == sale.Артикул_проданного_товара)
            item_result = await session.execute(item_stmt)
            item = item_result.scalar_one_or_none()
            if item:
                sales_amount += float(item.Оценочная_стоимость)
    
    report_data = {
        'quarter': quarter,
        'year': year,
        'total_loans': len(loans),
        'total_loan_amount': sum(float(loan.Размер_займа) for loan in loans),
        'paid_loans': len([l for l in loans if l.Статус_займа == 'Выплачен']),
        'overdue_loans': len([l for l in loans if l.Статус_займа == 'Просрочен']),
        'total_sales': len(sales),
        'sales_amount': sales_amount
    }
    
    return jsonify(report_data)

@app.route('/api/reports/loans-status')
async def loans_status_report():
    async with async_session_maker() as session:
        statuses_stmt = select(Loan.Статус_займа, func.count(Loan.Код_займа)).group_by(Loan.Статус_займа)
        statuses_result = await session.execute(statuses_stmt)
        statuses = statuses_result.all()
    return jsonify([{'status': s[0], 'count': s[1]} for s in statuses])

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5000)

