from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column
from sqlalchemy import Integer, String, Date, Numeric, ForeignKey
from datetime import date
from decimal import Decimal
import os
from config import config

# Базовый класс для моделей
class Base(DeclarativeBase):
    pass

# Создание асинхронного движка
config_name = os.getenv('FLASK_ENV', 'development')
config_obj = config[config_name]()
engine = create_async_engine(
    config_obj.DATABASE_URI,
    echo=False,
    pool_pre_ping=True,  # Проверка соединения перед использованием
    pool_size=5,  # Размер пула соединений
    max_overflow=10,  # Максимальное количество дополнительных соединений
    pool_recycle=3600  # Переподключение каждые 3600 секунд
)

# Создание фабрики сессий
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Функция для получения сессии
async def get_session():
    async with async_session_maker() as session:
        yield session

class Client(Base):
    __tablename__ = 'Клиент'
    
    ID_Клиента: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    ФИО: Mapped[str] = mapped_column(String(100), nullable=False)
    Телефон: Mapped[str] = mapped_column(String(16), nullable=False, unique=True)
    
    loans: Mapped[list['Loan']] = relationship('Loan', back_populates='client', lazy='selectin')
    
    def __repr__(self):
        return f'<Client {self.ФИО}>'

class Employee(Base):
    __tablename__ = 'Сотрудник'
    
    ID_Сотрудника: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    ФИО_Сотрудника: Mapped[str] = mapped_column(String(100), nullable=False)
    Должность: Mapped[str] = mapped_column(String(50), nullable=False)
    Дата_Приёма: Mapped[date] = mapped_column(Date, nullable=False, default=date(2025, 5, 1))
    Дата_Увольнения: Mapped[date | None] = mapped_column(Date, nullable=True)
    Телефон_Сотрудника: Mapped[str] = mapped_column(String(16), nullable=False, unique=True)
    Логин: Mapped[str | None] = mapped_column(String(50), nullable=True, unique=True)
    Пароль: Mapped[str | None] = mapped_column(String(255), nullable=True)
    
    loans: Mapped[list['Loan']] = relationship('Loan', back_populates='employee', foreign_keys='Loan.Исполнитель', lazy='selectin')
    sales: Mapped[list['Sale']] = relationship('Sale', back_populates='seller', lazy='selectin')
    
    def __repr__(self):
        return f'<Employee {self.ФИО_Сотрудника}>'
    
    def get_id(self):
        """Метод для Quart-Login"""
        return str(self.ID_Сотрудника)
    
    def is_active(self):
        """Проверка, активен ли сотрудник (не уволен)"""
        return self.Дата_Увольнения is None

class InterestRate(Base):
    __tablename__ = 'Процент_по_займу'
    
    Индекс_процента: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    Состояние_товара: Mapped[Decimal] = mapped_column(Numeric(4, 2), nullable=False)
    Срок_займа: Mapped[Decimal] = mapped_column(Numeric(4, 2), nullable=False)
    Процент: Mapped[Decimal] = mapped_column(Numeric(4, 2), nullable=False)
    
    loans: Mapped[list['Loan']] = relationship('Loan', back_populates='interest_rate', lazy='selectin')
    
    def __repr__(self):
        return f'<InterestRate {self.Индекс_процента}>'

class Loan(Base):
    __tablename__ = 'Займ'
    
    Код_займа: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    Дата_займа: Mapped[date] = mapped_column(Date, nullable=False)
    Клиент: Mapped[int] = mapped_column(Integer, ForeignKey('Клиент.ID_Клиента'), nullable=False)
    Размер_займа: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    Процент_по_займу: Mapped[int] = mapped_column(Integer, ForeignKey('Процент_по_займу.Индекс_процента'), nullable=False)
    Срок_займа: Mapped[Decimal] = mapped_column(Numeric(4, 2), nullable=False)
    Статус_займа: Mapped[str] = mapped_column(String(20), nullable=False)  # Переименовано из Состояние_товара
    Состояние_товара: Mapped[Decimal] = mapped_column(Numeric(4, 2), nullable=False)  # Процент из InterestRate
    Артикул_товара: Mapped[int] = mapped_column(Integer, nullable=False)
    Наименование_товара: Mapped[str] = mapped_column(String(200), nullable=False)
    Категория_товара: Mapped[str] = mapped_column(String(100), nullable=False)
    Физическое_состояние: Mapped[str] = mapped_column(String(50), nullable=False)
    Исполнитель: Mapped[int] = mapped_column(Integer, ForeignKey('Сотрудник.ID_Сотрудника'), nullable=False)
    
    client: Mapped['Client'] = relationship('Client', back_populates='loans', lazy='selectin')
    employee: Mapped['Employee'] = relationship('Employee', back_populates='loans', foreign_keys=[Исполнитель], lazy='selectin')
    interest_rate: Mapped['InterestRate'] = relationship('InterestRate', back_populates='loans', lazy='selectin')
    unclaimed_items: Mapped[list['UnclaimedItem']] = relationship('UnclaimedItem', back_populates='loan', foreign_keys='UnclaimedItem.Займ', lazy='selectin')
    
    def __repr__(self):
        return f'<Loan {self.Код_займа}>'

class UnclaimedItem(Base):
    __tablename__ = 'Невостребованный_товар'
    
    Артикул: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    Займ: Mapped[int] = mapped_column(Integer, ForeignKey('Займ.Код_займа'), nullable=False)
    Оценочная_стоимость: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    
    loan: Mapped['Loan'] = relationship('Loan', back_populates='unclaimed_items', foreign_keys=[Займ], lazy='selectin')
    sales: Mapped[list['Sale']] = relationship('Sale', back_populates='item', foreign_keys='Sale.Артикул_проданного_товара', lazy='selectin')
    
    def __repr__(self):
        return f'<UnclaimedItem {self.Артикул}>'

class Sale(Base):
    __tablename__ = 'Продажа'
    
    Код_продажи: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    Дата_продажи: Mapped[date] = mapped_column(Date, nullable=False, default=date.today)
    Артикул_проданного_товара: Mapped[int] = mapped_column(Integer, ForeignKey('Невостребованный_товар.Артикул'), nullable=False)
    Продавец: Mapped[int] = mapped_column(Integer, ForeignKey('Сотрудник.ID_Сотрудника'), nullable=False)
    
    item: Mapped['UnclaimedItem'] = relationship('UnclaimedItem', back_populates='sales', foreign_keys=[Артикул_проданного_товара], lazy='selectin')
    seller: Mapped['Employee'] = relationship('Employee', back_populates='sales', foreign_keys=[Продавец], lazy='selectin')
    
    def __repr__(self):
        return f'<Sale {self.Код_продажи}>'
