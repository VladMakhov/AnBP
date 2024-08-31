using System;
using System.Collections.Generic;
using Common.Logging;
using Terrasoft.Configuration;
using Terrasoft.Core;
using Terrasoft.Core.DB;
using Terrasoft.Core.Entities;
using Terrasoft.Core.Entities.Events;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

/**
 *                  | Документация бизнес-логики |
 *
 * Бизнес-процесс запускается при добавлении email Обращения
 * 
 * Читается Контакт из Обращения и происходит проверка Контакта на спам по следующем принципам
 * 1. В Контакте поле "Тип" имеет значение "Клиент не определен/Спам"
 * 2. В системной настройке "OLP: Этап 1" выставлено значение "false"
 * 
 * Если Контакт подходит под описание, то нужно *Выставить 1 линию поддержки 
 * с полем "Категория" установленном на значение "Новая командировка/Изменение командировки"
 * После чего Бизнес-процесс завершается
 * 
 * Если же Контакт не спам, то мы получаем данные email-а (Заголовок, тело, кому, копии и т.п) 
 * из объекта родительской Активности и выполняем следующие действия
 * 1. Достаем за базы данных все ОСНОВНЫЕ и ДЕЖУРНЫЕ группы обслуживания
 * 2. На основе полей "Кому" и "Копии" в email-е отбираем подходящие ОСНОВНЫЕ и ДЕЖУРНЫЕ группы обслуживания 
 * 
 * Далее мы получаем email "Адрес" и "Домен" из объекта Контакт
 * Делаем проверку на наличие в заголовке строк "TRAVEL-" или "ЗАЯВКА ПО РЕЛОКАЦИИ_"
 * Первая указывает на заявку от компании Яндекс, вторая от Сибур, которые обрабатываются отдельно
 * В Обращение устанавливаем поле "Номер TRAVEL" или "Релокация - СИБУР" (В зависимости от темы письма) 
 * на "Номер темы", которую вытаскиваем из "Заголовка" письма
 * 
 * После чего мы проверяем заполнены ли поля "Компания" и "Тип" в объекте Контакт
 * Если "Компания" указана, а "Тип" Контакта "Сотрудник" или "Поставщик" и системная настройка "OLP: Этап 1" не установлена
 *     *Выставить 1 линию поддержки с полем "Категория" установленном на значение "Сотрудник/Поставщик"
 *     После чего Бизнес-процесс завершается
 * 
 * Если ни "Компания", ни "Тип" не указаны
 *     Тогда пробуем найти домен Контакта среди доменов компании или среди почт компании. Делаем мы это так
 *     Достаем за базы данных Контрагента из объект "Средство связи контрагента" по следующим фильтрам
 *     1. В колонках "Тип" значение "Почтовый домен", а в "Номер" значение "Домен email" из объекта Контакт
 *     Или
 *     2. В колонках "Тип" значение "Email", а в "Номер" значение "Адрес" email-а из объекта Контакт
 *     После чего получаем поля "Тип" и "Холдинг" Контрагента из соответствующего объекта
 * 
 *     Проверяем найдена ли компания по домену контакта
 *     Если удалось найти Контрагента, у которого в поле "Тип" значение "Поставщик" или "Наша компания"
 *         Устанавливаем в поля Контакта "Тип" и "Компанию" значения Контрагента и 
 *         "Поставщик" или "Сотрудник", соответственно
 *         Так же, если не установлена системная настройка "OLP: Этап 1", то нужно 
 *             *Выставить 1 линию поддержки и завершить Бизнес-процесс
 *         Далее логика из этой части будет описана ниже в **Переход 2
 *     
 *     Если Компания или холдинг найдены, то выполняем логику описанную ниже в **Переход 1
 * 
 * Если установлена системная настройка "OLP: Этап 1" и "Тип" Контакта "Сотрудник" или "Поставщик"
 *     Запоминаем, что "Категорию обращение" в последствии будет установлена как "Сотрудник/Поставщик"
 *     После чего выполняем логику описанную ниже в **Переход 2
 * 
 * Если "Компания" указана и "Тип" Контакта "Клиент" или "Клиент не определен", то выполняем логику описанную ниже в **Переход 1
 * 
 * **Переход 1 (ЕИС - поиск контакта по Ид./Email/Телефону 1)
 * Выполняем запрос сервис ЕИС - поиск контакта по Ид./Email/Телефону по методу Поиск контакта по Ид./Email/телефону
 * передавая параметр "Адрес" email-а Контакта.
 * 
 * Проверяем найден ли Контакт в ЕИС
 * Если пришел НЕ успешный ответ от ЕИС, но Контрагент пустой и "Тип" Контакта пустой или "Клиент не определен/Спам"
 *     Категория обращения будет помечена как "Клиент не определен/Спам"
 *     Установка в объекте Контакт полей "Компания" и "Тип" на "OLP:AC Компания - не определена" и "Клиент не определен/Спам"
 *     
 *     Далее, Если системная настройка "OLP: Этап 1" не установлена
 *     то *Выставить 1 линию поддержки с "Категория обращения" как "Клиент не определен/Спам" и завершить Бизнес-процесс
 *     
 *     Eсли же установлена системная настройка "OLP: Этап 1", то
 *     установить в объекте Обращение полей "Категория", "Важность" и "Срочность"
 *     на "Клиент не определен/Спам", "Не важно" и "Не срочно"
 *     
 *     Последующая логика описана ниже в **Переход 2 
 * 
 * Если просто пришел успешный ответ от ЕИС или пришел успешный ответ от ЕИС и поле "Подтвержден в БД АК" Контакта установлено
 *     Получаем поля "Пилот" и "Холдинг" из объекта Контрагент, 
 *     у которого "Внешний Ид.БД АК" совпадает с "CompanyId" пришедшего из ЕИС
 * 
 *     Далее, для каждого email-а из списка email-ов, пришедших от ЕИС
 *         Выполняем запрос в базу данных по объекту "Средство связи контакта", у которого выставлены фильтры
 *         1. Поле "Contact" совпадает с Контактом
 *         2. Поле "Number" совпадает с "Адресом" email-а
 *         3. Поле "CommunicationType" или же тип связи выставлен как "Email"
 * 
 *         Если такого объекта нет, то добавляем его с вышеперечисленными полями
 * 
 *     То же самое действия проделываем с телефонами, пришедшими от ЕИС, заменяя параметр тип связи на "Phone"
 * 
 *     Обновляем данные Контакта
 * 
 *     Устанавливаем параметр "Ид. Холдинга компании клиента" на Контрагента для последующей установки параметра
 * 
 *     Последующая логика описана ниже в **Переход 3
 * 
 * Во всех остальных случаях
 *     Проверяем есть ли уже Компания
 *     Если Компания не установлена 
 *         Обновляем объект Контакт, устанавливая "Тип" на "Клиент", "Email" на "email" Контакта и "Компания" на "Контрагента"    
 *     Если Компания установлена 
 *         Обновляем объект Контакт, устанавливая "Тип" на "Клиент" и "Email" на "email" Контакта
 *     Последующая логика описана ниже в **Переход 2
 * 
 * **Переход 2 (Найти данные компании привязанной к контакту ненайденного в ЕИС)
 * Считываем объект Контрагент, у которого поле "Id" совпадает с объектом Обращение
 * После чего выставляем "Ид. Холдинга компании клиента" как "Холдинг" из объекта Контрагент
 * Последующая логика описана ниже в **Переход 3
 * 
 * **Переход 3 Чтение карточки контакта после обновления
 * Чтение объект Контакта после обновления данных
 * Выставляем в контекста процесса (Запоминяем для последующей установки в объект) параметры 
 * "Клиент ВИП платформа" на "Признак ВИП платформы",
 * "Признак ВИП" на "Признак ВИП",
 * "Ид. Компании клиента" на "Компания",
 * а так же в объект родительскую Активность выставляем "Контрагента" на "Компания" (Все параметры берем у Контакта)
 * 
 * Делаем запрос в бд по объекту "Группы обслуживания", добавляя следующие фильтры, 
 * если у Контакта не установлен параметр "ВИП платформа"
 * "Тип группы", указав выборку по "Основная группа"
 * "Почтовый ящик", указав выборку по "Почтовый ящик для регистрации обращений"
 * А так же сделать выборку "Компания" у контакта или "Холдинг" у Контрагента по параметру
 * "Компании" у объекта "Компании в группе обслуживания"
 * Если же у Контакта "ВИП платформа" установлена, то только добавить фильтр "Тип группы" на "ВИП Платформа" 
 * 
 * Далее для каджого элемента получившейся коллекции добавляем в список "Коллекция групп обслуживания"
 * новый элеметр у которого буду установлены параметры "Ид. Компании клиента" и "График работы", которые берем из ГО
 * Если нет ни одной подходящей ГО, установить параметр "Дежурную группу"
 * 
 * После чего идет проверка найдена ли группа по компании ВИП Платформа
 * Если не установлены параметр "Дежурная группа" и системная настройка "OLP: Этап 1"
 *     *Найти ГО основную по графику работы
 *     Далее логика описывается в **Переход 4
 * 
 * Если группа по компании ВИП Платформа не найдена, то 
 *     сразу переходим к логике в **Переход 4
 * 
 * Если установлена системная настройка "OLP: Этап 1"
 *     *Найти ГО основную по графику работы
 *     
 *     Какую ГО установить?
 *     Если установлены "Ид. экстра группы из кому/копии" и "Дежурная группа", а 
 *     "Ид. основной группы по почтовому ящику" не стоит
 *         Далее логика описывается в **Переход 5
 * 
 *     Если не установлена "Дежурная группа"
 *         (Отправить автоответ - не пишите на бук)
 *         В Активности установить поле Автоматическое уведомление
 *         Последующая логика описывается в **Переход 6
 * 
 *     Если установлены "Ид. основной группы по почтовому ящику" и "Дежурная группа"
 *         Если указаны "Ид. экстра группы из кому/копии" и "Дежурная группа"
 *             Далее логика описывается в **Переход 5
 * 
 *         Если установлена "Ид. выбранной ГО" и не стоит "Дежурная группа"
 *             Последующая логика описывается в **Переход 6
 * 
 *         Если не подходит осн по компании и та что указана в почте и нет дежурной в копии
 *             (Найти ГО дежурную по графику работы) вредо как по *Найти ГО основную по графику работы 
 * 
 *             Если не установлено "Ид. выбранной ГО" и
 *             Указана "Дежурная группа" или не указана и "Email" Контакта содержит "NOREPLY@" или "NO-REPLY@" или "EDM@npk.team"
 *                 Последующая логика описывается в **Переход 6
 * 
 *             Если не установлена "Дежурная группа" и "Email" Контакта не содержит "NOREPLY@" или "NO-REPLY@" или "EDM@npk.team"
 *                 Отправить автоответ на букинг
 *                 В Активности установить поле Автоматическое уведомление
 *                 Последующая логика описывается в **Переход 6
 * 
 *     В противном случае **Переход 7
 * 
 * **Переход 4 (Какая ГО должна быть у контакта?)
 * Если установлены параметры "Дежурная группа" и "Клиент ВИП", то 
 *     установить в "Ид. выбранной ГО" значение "OLP:ГО Дежурная группа" и **Переход 5
 * 
 * Если не стоит "Дежурная группа", то **Переход 5
 * 
 * Если установлен параметр "Дежурная группа", но не "Клиент ВИП"
 *     установить в "Ид. выбранной ГО" значение "OLP:ГО Общая 1 линия поддержки"
 *     *Добавить 1 линия поддержки на обр
 *     **Переход 7
 * 
 * **Переход 5 (Найти ГО дежурную из кому/копии по графику работы)
 * (Найти ГО дежурную из кому/копии по графику работы) Похоже на *Найти ГО основную по графику работы с заменой "Основной", а "Дежурную"
 * 
 * Если Дежурная ГО найдена, то **Переход 6
 * 
 * Если Дежурной ГО нет
 *     *Найти основную ГО для контакта по компаниям
 *     Если Дежурной ГО нет
 *         То установить "Ид. выбранной ГО" на "Ид. экстра группы из кому/копии"
 *     (Отправить автоответ - не пишите на бук)
 *     В Активности установить поле Автоматическое уведомление
 *     Последующая логика описывается в **Переход 6
 * 
 * **Переход 6 (Найти Группу обслуживания по Ид)
 * Читаем объект Группы обслуживания, которуя раньше выудили из проверок, она находиться в "Ид. выбранной ГО"
 * Если "Тип" Группы обслуживания это "ВИП Платформа"
 *     Устанавливаем "Важность для выставления" как "Важно"
 *     *Добавить 2 линия поддержки на обр
 *     **Переход 7
 * 
 * Если установлены "Клиент ВИП" у ГО
 *     Устанавливаем "Важность для выставления" как "Важно"
 *     Если "Распределение ВИП" у ГО
 *         (Найти 3 линию поддержки для обращения (старший агент))
 *         Если нашли *Добавить 3 линия поддержки на обр и **Переход 7
 *     Далее *Добавить 2 линия поддержки на обр и **Переход 7
 * 
 * (Найти 1 линию поддержки для обращения)
 * Если "Приоритет" у родительской Активности "Высокий" - Установить "Важность для выставление" как "Важно"
 * 
 * Если найдена 1 линия поддержки - *Добавить 2 линия поддержки на обр и **Переход 7
 * 
 * Если не найдена 1 линия поддержки и установлен "OLP: Этап 1"
 *     установить в "Ид. выбранной ГО" значение "OLP:ГО Общая 1 линия поддержки"
 *     *Добавить 1 линия поддержки на обр
 *     **Переход 7
 * 
 * **Переход 7
 * Если не успешный ответ от ЕИС, то завершить БП
 * Если "OrderNumbCheck" от ЕИС не пустой
 *     (Собрать услуги для добавления)
 *     Запустить "OLP: Подпроцесс - Обновление услуг контакта v 3.0.1"
 * Завершить БП
 * 
 * *Выставить 1 линию поддержки
 * Изменить в объекте Обращение поля:
 * 1. "Категория" на ту категорию, которая установлена в данном контексте 
 * (по умолчанию "Новая командировка/Изменение командировки")
 * 2. "Группа" обслуживания на "OLP:ГО Общая 1 линия поддержки"
 * 3. "Линия" поддержки на "OLP:ОР 1 линия поддержки"
 * 4. "Важность" на "Не важно"
 * 5. "Срочность" на "Не срочно"
 * 
 * *Найти ГО основную по графику работы 
 * Если "График работы" установлен как "Круглосуточный" сразу запоминаем ГО и идем дальше, пропуская шаги ниже 
 * Для каждого элемента Коллекции групп обслуживания проверяем следующие условия
 * 1. Если дата обращение совпадает с выходным-праздничным днем, то переходим к следующей ГО
 * 2. Если дата обращение совпадает с праздничным днем, который рабочий, то записать текущую ГО
 * 3. Если дата обращение подходит под график работы текущей ГО, то используем эту конкрутную ГО
 * в противном случаем переходим на "Дежурную групп"
 * 
 * *Найти основную ГО для контакта по компаниям
 * Делаем запрос в бд по объекту "Группы обслуживания", добавляя следующие фильтры, 
 * если у Контакта не установлен параметр "ВИП платформа"
 * "Тип группы", указав выборку по "Основная группа"
 * "Почтовый ящик", указав выборку по "Почтовый ящик для регистрации обращений"
 * А так же сделать выборку "Компания" у контакта или "Холдинг" у Контрагент по параметру
 * "Компании" у объекта "Компании в группе обслуживания"
 * Если же у Контакта "ВИП платформа" установлена, то только добавить фильтр "Тип группы" на "ВИП Платформа"
 * 
 * *Добавить 1 линия поддержки на обр
 * Изменить Обращение в параметрах
 * 1. "Линия поддержки" на "OLP:ОР 1 линия поддержки"
 * 2. "Важность" на "Важность для выставления" (из контекста текущего разветвления)
 * 3. "Срочность" на "Не срочно"
 * 4. "Признак ВИП у Автора" на "Клиент ВИП" (из контекста текущего разветвления)
 * 5. "Группа обслуживания" на "Ид. выбранной ГО"
 * 6. "Компания" на "Компания" у Контакта
 * 7. "Категория" на "Категория обращения" (из контекста текущего разветвления)
 * 8. "Группа обслуживания (основная для очереди)" на "Группа обслуживания для выставления очереди" (из контекста текущего разветвления) 
 * 
 * *Добавить 2 линия поддержки на обр
 * Изменить Обращение в параметрах как в *Добавить 1 линия поддержки на обр, но
 * "Линия поддержки" на "OLP:ОР 2 линия поддержки"
 * 
 * *Добавить 3 линия поддержки на обр
 * Изменить Обращение в параметрах как в *Добавить 1 линия поддержки на обр, но
 * "Линия поддержки" на "OLP:ОР 2 линия поддержки"
 * 1. "Линия поддержки" на "OLP:ОР 3 линия поддержки"
 * 2. "Агент" на "Старший агент" 3-й линии
 *
 * */
namespace AnseremPackage
{

    [EntityEventListener(SchemaName = nameof(Case))]
    private class AnEmailCaseProcessor: BaseEntityEventListener
    { 
        /**CONSTS**/
        private const Guid SERVICE_GROUP_TYPE_MAIN = new Guid("C82CA04F-5319-4611-A6EE-64038BA89D71");

        private const Guid SERVICE_GROUP_TYPE_EXTRA = new Guid("DC1B5435-6AA1-4CCD-B950-1C4ADAB1F8AD");

        private const Guid CONTACT_TYPE_SUPPLIER = new Guid("260067DD-145E-4BB1-9C91-6BF3A36D57E0");

        private const Guid CONTACT_TYPE_EMPLOYEE = new Guid("60733EFC-F36B-1410-A883-16D83CAB0980");

        private const Guid CONTACT_TYPE_CLIENT = new Guid("00783ef6-f36b-1410-a883-16d83cab0980");

        private const Guid OLP_DUTY_SERVICE_GROUP = new Guid("3A8B2C01-7A4D-4D5D-A7EA-8563BF6220B9"); 

        // OLP:ГО Общая 1 линия поддержки
        private const Guid OLP_GENERAL_FIRST_LINE_SUPPORT = new Guid("64833178-8B17-4BB6-8CD9-6165B9B82637"); 

        // OLP:ОР 1 линия поддержки
        private const Guid OLP_OR_FIRST_LINE_SUPPORT = new Guid("B401FC39-77E4-4B53-985F-21E68947A107"); 

        // OLP:ОР 2 линия поддержки
        private const Guid OLP_OR_SECOND_LINE_SUPPORT = new Guid("DF594796-8E36-41BC-8EDD-732967053947"); 

        // OLP:ОР 3 линия поддержки (старшие агенты)
        private const Guid OLP_OR_THIRD_LINE_SUPPORT = new Guid("3D0C8864-BF2F-4734-8A29-31873EB07440"); 

        private const Guid CASE_URGENCY_TYPE_NOT_URGENT = new Guid("7a469f22-111d-4749-b5c2-e2a109a520a0");

        private const Guid CASE_URGENCY_TYPE_URGENT = new Guid("97c567ad-dbf8-4923-a766-c49a85b3ebdf");

        private const Guid CASE_IMPORTANCY_IMPOTANT = new Guid("fd6b8923-4af8-48f9-8180-b6e1da3a1e2d");

        private const Guid CASE_IMPORTANCY_NOT_IMPOTANT = new Guid("007fc788-5edd-42dd-a9ac-c56d010e7205");

        private const Guid VIP_PLATFORM = new Guid("97c567ad-dbf8-4923-a766-c49a85b3ebdf");

        private const Guid ACTIVITY_PRIORITY_HIGH = new Guid("D625A9FC-7EE6-DF11-971B-001D60E938C6");

        private const Guid ACTIVITY_PRIORITY_MEDIUM = new Guid("AB96FA02-7FE6-DF11-971B-001D60E938C6");

        private const Guid ACTIVITY_PRIORITY_LOW = new Guid("AC96FA02-7FE6-DF11-971B-001D60E938C6");

        private const Guid CONTACT_TYPE_UNDEFINED_CLIENT_SPAM = new Guid("1a334238-08ba-466d-8d40-a996afcb8fe1");

        private const Guid CASE_CATEGORY_EMPLOYEE_SUPPLIER = new Guid("84f67e2e-842e-47ae-99aa-882d1bc8e513");

        private const Guid ACCOUNT_TYPE_SUPPLIER = new Guid("1414f55f-21d2-4bb5-847a-3a0681d0a13a");

        private const Guid ACCOUNT_TYPE_OUR_COMPANY = new Guid("57412fad-53e6-df11-971b-001d60e938c6");
        
        private const Guid CASE_STATUS_CLOSED = new Guid("ae7f411e-f46b-1410-009b-0050ba5d6c38");
        
        private const Guid CASE_STATUS_CANCELED = new Guid("6e5f4218-f46b-1410-fe9a-0050ba5d6c38");
 
        private const Guid COMMUNICATION_TYPE_EMAIL_DOMAIN = new Guid("9E3A0896-0CBE-4733-8013-1E70CB09800C");
        
        private const Guid COMMUNICATION_TYPE_EMAIL = new Guid("EE1C85C3-CFCB-DF11-9B2A-001D60E938C6");
        
        private const Guid ACTIVITY_TYPE_EMAIL = new Guid("E2831DEC-CFC0-DF11-B00F-001D60E938C6");
        
        private const Guid OLP_AC_COMPANY_IS_NOT_DEFINED = new Guid("A1998F90-2EEC-48A4-94E8-A3CF48134FFB");
        /**CONSTS**/
 
        /**SYS_SETTINGS**/
        private bool isOlpFirstStage = GetOlpFirstStage();

        private bool LoadingCheck = GetLoadingCheck(); 
        /**SETTINGS**/
        
        /**PARAMS**/
        private string caseCategory { get; set; }

        private Contact contact { get; set; }
        
        private Guid contactId { get; set; }

        private object activity { get; set; }

        private Profile eis { get; set; }

        private object account { get; set; }

        private Guid caseId { get; set; }

        private Guid parentActivityId { get; set; }

        private Guid mainServiceGroup { get; set; }

        private Guid extraServiceGroup { get; set; }

        private Guid holding { get; set; }

        private Guid clientCompanyId { get; set; }

        private Guid selectedServiceGroupId { get; set; }

        private bool clientVipPlatform { get; set; }

        private bool clientVip { get; set; }

        private bool isExtraServiceGroup { get; set; }

        private Guid importancy { get; set; }

        private Guid urgency { get; set; }
        
        private Guid email { get; set; }
        
        private Guid copies { get; set; }
        
        private Guid mailto { get; set; }
        /**PARAMS**/

        /**LEGACY**/
        private Guid MainGroupIdByEmail { get; set; }
        
        private string MainGroupEmailBox { get; set; }
        
        private Guid MainGroupEmailBoxId { get; set; }
        
        private Guid MainEmailBoxIdForReg { get; set; }
        
        private string MainSheduleTypeByMail { get; set; }
        
        private string ExtraGroupEmailBox { get; set; }
        
        private Guid ExtraGroupIdByEmail { get; set; }
        
        private Guid ExtraGroupEmailBoxId { get; set; }
        
        private string ExtraSheduleTypeByMail { get; set; }
        
        private Guid ContactIdForEmailAndPhone { get; set; }
        
        private CompositeObjectList<CompositeObject> ProcessSchemaParameterServiceGroupCollection { get; set; }
        
        private bool ProcessSchemaParameterIsDutyGroup { get; set; }
        
        private Guid ProcessSchemaParamServiceGroupId { get; set; }

        private int ProcessSchemaParameter2 { get; set; }
        
        private int ProcessSchemaParameter3 { get; set; }

        private string DateFromForReply { get; set; }
        
        private string  DateToForReply { get; set; }
        
        private Guid ServiceGroupForOrder { get; set; }
 
        private bool ProcessSchemaParamCompanyFoundId { get; set; }

        private bool ProcessSchemaParamHoldingFoundId { get; set; }

        private CompositeObjectList<CompositeObject> ServicesToAdd { get; set;}
        
        private string ProcessSchemaParamTime { get; set;}
        /**LEGACY**/
        
        public override void OnInserting(object sender, EntityAfterEventArgs e)
        {
            base.OnInserting(sender, e);
            var _case = (Entity)sender;
            caseId = _case.GetTypedColumnValue<Guid>("Id");

            // Чтение карточки контакта из обращения
            contact = ReadContactFromCase(_case.GetTypedColumnValue<Guid>("ContactId"));
            
            contactId = contact.GetTypedColumnValue<Guid>("contactId");

            // Нет (СПАМ) - 2 ЭТАП
            if (contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_UNDEFINED_CLIENT_SPAM) 
            {
                // Спам на обращении + 1 линия поддержки
                UpdateCaseToFirstLineSupport();
                return;
            }

            parentActivityId = _case.GetTypedColumnValue<Guid>("ParentActivityId");
            
            // email родительской активности
            activity = GetParentActivityFromCase();

            email = activity.GetTypedColumnValue<string>("Sender");

            mailto = activity.GetTypedColumnValue<string>("Recepient"); 

            copies = activity.GetTypedColumnValue<string>("CopyRecepient"); 
            
            /**
             * Чтение всех основных групп для выделения подходящей основной группы
             * Найти основную группу по email в кому/копия
             */
            mainServiceGroup = GetServiceGroupMain();

            /**
             * Чтение дежурной группы
             * Найти дежурную группу по email в кому/копия
             */
            extraServiceGroup = GetServiceGroupExtra();

            /**
             * Добавить TRAVEL
             * Поставить отменено на всех отмененных тревел обращениях 
             */  
            SetTravelParameter(); 

            /**
             * Добавить Релокация - СИБУР
             * Поставить отменено на всех отмененных тревел обращениях - Копия 
             */
            SetSiburParameter(); 

            // Да (Сотрудник) - 2 ЭТАП
            if (contact.GetTypedColumnValue<Guid>("Account") != Guid.Empty && !isOlpFirstStage && 
                    (contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_EMPLOYEE || contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_SUPPLIER))
            {
                caseCategory = CASE_CATEGORY_EMPLOYEE_SUPPLIER; 
                /**
                 * Категория (Сотрудник/Поставщик) -2 этап
                 * Выставить 1 линию поддержки
                 */
                UpdateCaseToFirstLineSupport();
                return;
            }

            // 1 Этап (Сотрудник/Поставщик)
            if (isOlpFirstStage && (contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_EMPLOYEE || contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_SUPPLIER))
            {
                caseCategory = CASE_CATEGORY_EMPLOYEE_SUPPLIER;
                goto2();
            }

            // Нет (Новый)
            if (contact.GetTypedColumnValue<Guid>("Account") == Guid.Empty || contact.GetTypedColumnValue<Guid>("Type") == Guid.Empty)
            {
                account = FetchAccountById(GetAccountIdFromAccountCommunication());

                // Да (Поставщик)
                if (account != null && account.GetTypedColumnValue<Guid>("Type") == ACCOUNT_TYPE_SUPPLIER)
                {
                    caseCategory = CASE_CATEGORY_EMPLOYEE_SUPPLIER;
                    SetContactType(contact.GetTypedColumnValue<Guid>("Id"), CONTACT_TYPE_SUPPLIER);
                    if (isOlpFirstStage)
                    {
                        goto2();
                    }
                    else
                    {
                        UpdateCaseToFirstLineSupport();
                    }
                }

                // Да (Аэроклуб)
                if (account != Guid.Empty && account.GetTypedColumnValue<Guid>("Type") == ACCOUNT_TYPE_OUR_COMPANY)
                {
                    caseCategory = CASE_CATEGORY_EMPLOYEE_SUPPLIER;
                    SetContactType(contact.GetTypedColumnValue<Guid>("Id"), CONTACT_TYPE_EMPLOYEE);
                    if (isOlpFirstStage)
                    {
                        goto2();
                    }
                    else
                    {
                        UpdateCaseToFirstLineSupport();
                    }
                }

                // Да (Компания/Холдинг) или потенциальный СПАМ
                EisPath();
            }

            // Да (Клиент, СПАМ)
            if (contact.GetTypedColumnValue<Guid>("Account") != Guid.Empty && 
                    (contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_CLIENT || contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_UNDEFINED_CLIENT_SPAM))
            {
                EisPath();
            }
        }

        private void EisPath()
        {
            isResponseSuccessful = SendEisRequest();

            // Да
            if (isResponseSuccessful || (isResponseSuccessful && contact.aeroclubCheck))
            {
                account = FetchAccountByEis(eis.Company);

                RefreshEmailsAndPhones();

                RefreshContact(account.GetTypedColumnValue<Guid>("Id"), contact.GetTypedColumnValue<Guid>("Id"));

                holding = account.GetTypedColumnValue<Guid>("OlpHolding");

                // Чтение карточки контакта после обновления
                ReadContactAfterRefreshing();
            }

            // Нет по домену и нет по ЕИС и (пустой тип или СПАМ)
            if (isResponseSuccessful && contact.account == Guid.Empty && (contact.GetTypedColumnValue<Guid>("Type") == Guid.Empty || contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_UNDEFINED_CLIENT_SPAM))
            {
                // категория СПАМ
                caseCategory = CONTACT_TYPE_UNDEFINED_CLIENT_SPAM;
                if (isOlpFirstStage)
                {
                    SetSpamOnCase();
                    goto2();
                }
                else
                {
                    SetSpamOnCase();   
                    return;
                }
            }

            // Нет по ЕИС
            // Да
            if (contact.GetTypedColumnValue<Guid>("Account") != Guid.Empty)
            {
                // Актуализировать тип контакта + Email 
                RefreshContactTypeAndEmail();
            }
            // Нет
            else
            {
                // Актуализировать компанию контакта + Email
                RefreshContactCompanyAndEmail();
            }

            goto2();
        }

        private void goto2()
        {
            // Найти данные компании привязанной к контакту ненайденного в ЕИС
            // Выставить холдинг компании контакта
            holding = GetHoldingFromAccountBindedToEis();            

            // Чтение карточки контакта после обновления
            ReadContactAfterRefreshing();
        }

        /** 
         * Чтение карточки контакта после обновления
         */
        private void ReadContactAfterRefreshing()
        {
            GetContactAfterRefreshing(contact.GetTypedColumnValue<Guid>("Id"));

            clientVip = contact.GetTypedColumnValue<bool>("OlpSignVip");

            clientVipPlatform = contact.GetTypedColumnValue<bool>("OlpSignVipPlatf");

            clientCompanyId = contact.GetTypedColumnValue<bool>("Account");

            AddAccountToEmail();

            // Найти основную ГО для контакта по компаниям и ВИП Платформа
            GetMainServiceGroup();

            // Найдена ли группа по компании ВИП Платформа?
            // Да
            if (!isExtraServiceGroup && !isOlpFirstStage)
            {
                // Найти ГО основную по графику работы 
                GetMainServiceGroupBasedOnTimetable(); 
                goto4();
            }

            // Этап 1
            if (isOlpFirstStage)
            {
                // Есть ГО
                if (!isExtraServiceGroup)
                {
                    // Найти ГО  основную по графику работы 1 этап
                    GetMainServiceGroupBasedOnTimetableOlpFirstStage(); 
                }

                // Какую ГО установить?
                // Указана только дежурная в кому/копии
                if (mainServiceGroup == Guid.Empty && extraServiceGroupFromAndCopy != Guid.Empty && isExtraServiceGroup) // Разобраться с параметрами
                {
                    goto5();
                }

                // Найдена ГО по компании и графику клиента и почтовому адресу
                else if (!extraServiceGroup)
                {
                    SendBookAutoreply(); // TODO Переделать под кастомные автоответы!
                    SetAutonotification();
                    goto6();
                }

                // Указана осн ГО в кому/копии
                else if (mainServiceGroup == Guid.Empty && isExtraServiceGroup)
                {
                    // Найти осн из почты по графику
                    GetMainServiceGroupFromMainBasedOnTimeTable(); 
                    if (isExtraServiceGroup && extraServiceGroupFromAndCopy)
                    {
                        goto5();
                    }
                    else if (selectedServiceGroupId && !extraServiceGroup)
                    {
                        goto6();
                    }
                    else
                    {
                        // Найти ГО дежурную по графику работы
                        GetExtraServiceGroupBaseOnTimetable();
                        
                        if (selectedServiceGroupId && (!extraServiceGroup || contact.email.contains("NOREPLY@") || contact.email.contains("NO-REPLY@") || contact.email.contains("EDM@npk.team")))
                        {
                            goto6();
                        }
                        else if (!selectedServiceGroupId && (!contact.email.contains("NOREPLY@") && !contact.email.contains("NO-REPLY@") && !contact.email.contains("EDM@npk.team")))
                        {
                            SendBookAutoreply(); // TODO Переделать под кастомные автоответы!
                            SetAutonotification();
                            goto6();
                        }
                    }
                }
                else
                {
                    goto7();
                }
            }

            // Нет
            else 
            {
                goto4();
            }
        }

        private void goto4()
        {
            // Дежурная ГО 2 линия поддержки
            if (extraServiceGroup && isVipClient)
            {
                selectedServiceGroupId = OLP_DUTY_SERVICE_GROUP;
                goto5();
            }

            // Основная клиентская/ВИП Платформа
            else if (!extraServiceGroup)
            {
                goto5();
            }

            // Общая 1 линия поддержки
            else if (extraServiceGroup && isVipClient)
            {
                selectedServiceGroupId = OLP_GENERAL_FIRST_LINE_SUPPORT;
                SetFirstLineSupport();
                goto7();
            }
        }

        private void goto5()
        {
            // Найти ГО дежурную из кому/копии по графику работы
            GetExtraServiceGroupFromFromAndCopyBaseOnTimetable();
            
            // дежурная ГО найдена или есть основная почта
            if (selectedServiceGroupId)
            {
                goto6();
            }
            else
            {
                // Найти основную ГО для контакта по компаниям
                GetMainServiceGroupForContactBasedOnCompany(); 

                selectedServiceGroupId = selectedServiceGroupId == Guid.Empty ? etraServiceGroupFromAndCopy : selectedServiceGroupId;
                
                SendBookAutoreply(); // TODO Переделать под кастомные автоответы!
                SetAutonotification();
                goto6();
            }
        }

        private void goto6()
        {
            var serviceGroup = GetServiceGroupBySelectedId();

            // Выбранная ГО ВИП Платформа?
            // Да
            if (serviceGroup.GetTypedColumnValue<bool>("OlpTypeGroupService") == VIP_PLATFORM)
            {
                importancy = CASE_IMPORTANCY_IMPOTANT;
                SetSecondLineSupport();
                goto7();
            }

            if (clientVip)
            {
                importancy = CASE_IMPORTANCY_IMPOTANT;

                if (serviceGroup.GetTypedColumnValue<bool>("OlpDistribution"))
                {
                    var thirdLineSupport = GetThirdLineSupport();
                    if (thirdLineSupport != Guid.Empty)
                    {
                        SetThirdLineSupport();
                        goto7();
                    }
                }
                SetSecondLineSupport();
                goto7();
            }

            var firstLineSupport = GetFirstLineSupport(); // TODO

            if (activity.GetTypedColumnValue<Guid>("Priority") == ACTIVITY_PRIORITY_HIGH)
            {
                importancy = CASE_IMPORTANCY_IMPOTANT;
            }

            if (firstLineSupport)
            {
                SetSecondLineSupport();
                goto7();
            }
            if (!firstLineSupport && isOlpFirstStage)
            {
                selectedServiceGroupId = OLP_GENERAL_FIRST_LINE_SUPPORT;
                SetFirstLineSupport();
                goto7();
            }
        }

        private void goto7()
        {
            if (isResponseSuccessful)
            {
                return;
            }
       
            // TODO
            // if (eis.orderNumbCheck != Guid.Empty) 
            // {
                // Собрать услуги для добавления
                CollectServicesForInsertion();

                // TODO Запустить "OLP: Подпроцесс - Обновление услуг контакта v 3.0.1"
                IProcessEngine processEngine = userConnection.ProcessEngine;
                IProcessExecutor processExecutor = processEngine.ProcessExecutor;

                try
                {
                    processExecutor.Execute(
                            "_PROCESS",
                            new Dictionary<string, string> { {"_PARAMETR", _KEY} }
                            );
                }
                catch (Exception e)
                {

                }
            //}

            return;
        }

        private bool GetOlpFirstStage()
        {
            string sql = @$"
                SELECT 
                    BooleanValue 
                FROM 
                    SysSettingsValue 
                WHERE 
                    SysSettingsId = (
                        SELECT id FROM SysSettings WHERE Code LIKE 'OLPIsFirstStepToPROD'
                        )
                ";

            CustomQuery query = new CustomQuery(UserConnection, sql);

            using (var db = UserConnection.EnsureDBConnection())
            {
                using (var reader = sql.ExecureReader(db))
                {
                    if (reader.Read())
                    {
                        return reader.GetColumnValue<bool>("BooleanValue");
                    }
                }
            }
        }

        private Contact ReadContactFromCase(Guid contactId)
        {
            var contact = new Contact(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { nameof(Contact.Id), contactId}, 
            };

            if (contact.FetchFromDB(conditions))
            {
                return contact;
            }
        }

        private void UpdateCaseToFirstLineSupport()
        {
            string sql = @$"
                UPDATE 
                    \"Case\"
                SET 
                    OlpGroupServices = '{OLP_GENERAL_FIRST_LINE_SUPPORT}', 
                    OlpSupportLine = '{OLP_OR_FIRST_LINE_SUPPORT}',
                    OlpImportant = '{CASE_IMPORTANCY_NOT_IMPOTANT}',
                    OlpUrgency = '{CASE_URGENCY_TYPE_NOT_URGENT}',
                    Category = '{caseCategory}'
                WHERE 
                    Id = '{caseId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private Activity GetParentActivityFromCase()
        {
            var activity = new Activity(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { nameof(Activity.Id), contactId },
                    { nameof(Activity.TypeId) ACTIVITY_TYPE_EMAIL} 
            };

            if (activity.FetchFromDB(conditions))
            {
                return activity;
            }
        }

        // Найти дежурную группу по email в кому/копия
        private Guid GetServiceGroupExtra()
        {
            string sql = @$"
                SELECT * FROM OlpServiceGroup
                WHERE Id IS NOT NULL AND
                OlpSgEmail IS NOT NULL AND
                OlpTypeGroupService = '{SERVICE_GROUP_TYPE_EXTRA}' 
                ";

            CustomQuery query = new CustomQuery(UserConnection, sql);

            using (var db = UserConnection.EnsureDBConnection())
            {
                using (var reader = sql.ExecureReader(db))
                {

                    Guid ExtraGroupIdTemp = System.Guid.Empty;

                    while (reader.Read())
                    {
                        /**LEGACY**/
                        string EmailBoxName = "";
                        string EmailBoxALias = "";
                        
                        ExtraGroupId = reader.GetColumnValue<string>("ExtraGroupId");

                        ExtraEmailBox = reader.GetColumnValue<string>("ExtraGroupEmailBoxId");

                        //ищем текстовое значение ящика по ГО
                        EntitySchemaQuery ExtraEmailBoxString = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "MailboxForIncidentRegistration");
                        ExtraEmailBoxString.PrimaryQueryColumn.IsAlwaysSelect = true;
                        ExtraEmailBoxString.ChunkSize = 1;
                        ExtraEmailBoxString.Filters.Add(ExtraEmailBoxString.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ExtraGroupEmailBoxId));
                        ExtraEmailBoxString.AddColumn("Name");
                        ExtraEmailBoxString.AddColumn("AliasAddress");

                        var MailboxSyncSettings = ExtraEmailBoxString.AddColumn("MailboxSyncSettings.Id"); //Ид. ящика из настройки почтовых ящиков
                        EntityCollection CollectionEmailText = ExtraEmailBoxString.GetEntityCollection(UserConnection);
                        //Найден ящик для регистрации обращений
                        if (CollectionEmailText.IsNotEmpty())
                        {
                            foreach (var itemsemail in CollectionEmailText)
                            {
                                EmailBoxName = itemsemail.GetTypedColumnValue<string>("Name"); //название в справочнике ящиков для рег. обращений
                                EmailBoxALias = itemsemail.GetTypedColumnValue<string>("AliasAddress");
                                if( !string.IsNullOrEmpty(EmailBoxALias)){    
                                    string[] words = EmailBoxName.Split('(');
                                    EmailBoxName = words[0];}

                                if (!string.IsNullOrEmpty(EmailBoxALias) && (email.ToUpper().Contains(EmailBoxName.ToUpper().Trim())
                                            || email.ToUpper().Contains(EmailBoxALias.ToUpper().Trim())
                                            || copies.ToUpper().Contains(EmailBoxALias.ToUpper().Trim())
                                            || copies.ToUpper().Contains(EmailBoxName.ToUpper().Trim())))
                                {
                                    ExtraGroupIdByEmail = ExtraGroupId;
                                    ExtraGroupEmailBox = itemsemail.GetTypedColumnValue<string>("Name");
                                    ExtraGroupEmailBoxId = itemsemail.GetTypedColumnValue<Guid>(MailboxSyncSettings.Name);
                                    ExtraGroupIdTemp = ExtraGroupId;
                                }
                                else if(email.ToUpper().Contains(EmailBoxName.ToUpper().Trim())
                                        || copies.ToUpper().Contains(EmailBoxName.ToUpper().Trim()))
                                {
                                    ExtraGroupIdByEmail = ExtraGroupId;
                                    ExtraGroupEmailBox = itemsemail.GetTypedColumnValue<string>("Name");
                                    ExtraGroupEmailBoxId = itemsemail.GetTypedColumnValue<Guid>(MailboxSyncSettings.Name);
                                    ExtraGroupIdTemp = ExtraGroupId;
                                }
                            }
                        }

                        if (ExtraGroupIdTemp != Guid.Empty)
                        {

                            EntitySchemaQuery esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                            esq.PrimaryQueryColumn.IsAlwaysSelect = true;
                            esq.ChunkSize = 1;
                            var OlpTypeScheduleWorks = esq.AddColumn("OlpTypeScheduleWorks.Name");
                            esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ExtraGroupIdTemp));
                            EntityCollection entityCollection = esq.GetEntityCollection(UserConnection);
                            //Идем в цикл если коллекция не пустая
                            if (entityCollection.IsNotEmpty()) 
                            {
                                foreach (var groupsshedule in entityCollection) 
                                {
                                    ExtraSheduleTypeByMail = groupsshedule.GetTypedColumnValue<string>(OlpTypeScheduleWorks.Name);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            return;
            /**LEGACY**/
        }
        
        // Найти основную группу по email в кому/копия
        private Guid GetServiceGroupMain()
        {
            string sql = @$"
                SELECT * FROM OlpServiceGroup
                WHERE Id IS NOT NULL AND
                OlpSgEmail IS NOT NULL AND
                OlpTypeGroupService = '{SERVICE_GROUP_TYPE_MAIN}' 
                "; 

            CustomQuery query = new CustomQuery(UserConnection, sql);

            using (var db = UserConnection.EnsureDBConnection())
            {
                using (var reader = sql.ExecureReader(db))
                {
                    Guid MainGroupIdTemp = Guid.Empty;

                    while (reader.Read())
                    {

                        /**LEGACY**/
                        string EmailBoxName = "";
                        string EmailBoxALias = "";

                        // Ид.ГО
                        Guid MainGroupId = reader.GetColumnValue<Guid>("MainGroupId");

                        // Ид.почтового ящика основной группы
                        Guid MainGroupEmailBoxId = reader.GetColumnValue<Guid>("MainGroupEmailBoxId");

                        //ищем текстовое значение ящика по ГО
                        EntitySchemaQuery EsqEmailBoxString = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "MailboxForIncidentRegistration");
                        EsqEmailBoxString.PrimaryQueryColumn.IsAlwaysSelect = true;
                        EsqEmailBoxString.ChunkSize = 1;
                        EsqEmailBoxString.Filters.Add(EsqEmailBoxString.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", MainGroupEmailBoxId));
                        EsqEmailBoxString.AddColumn("Name");
                        EsqEmailBoxString.AddColumn("AliasAddress");

                        var MailboxSyncSettings= EsqEmailBoxString.AddColumn("MailboxSyncSettings.Id"); //Ид. ящика из настройки почтовых ящиков

                        EntityCollection CollectionEmailText = EsqEmailBoxString.GetEntityCollection(UserConnection);

                        //Найден ящик для регистрации обращений
                        if (CollectionEmailText.IsNotEmpty()){
                            foreach (var itemsemail in CollectionEmailText){
                                EmailBoxName = itemsemail.GetTypedColumnValue<string>("Name"); //название в справочнике ящиков для рег. обращений
                                EmailBoxALias = itemsemail.GetTypedColumnValue<string>("AliasAddress");
                                //	var servicegroupid = itemsemail.GetTypedColumnValue<Guid>("Id");

                                if( !string.IsNullOrEmpty(EmailBoxALias))
                                {
                                    string[] words = EmailBoxName.Split('(');
                                    EmailBoxName = words[0];
                                }

                                if (!string.IsNullOrEmpty(EmailBoxALias) && (email.ToUpper().Contains(EmailBoxName.ToUpper().Trim())
                                            || email.ToUpper().Contains(EmailBoxALias.ToUpper().Trim())
                                            || copies.ToUpper().Contains(EmailBoxALias.ToUpper().Trim())
                                            || copies.ToUpper().Contains(EmailBoxName.ToUpper().Trim())))
                                {
                                    MainGroupIdByEmail = MainGroupId;
                                    MainGroupEmailBox = itemsemail.GetTypedColumnValue<string>("Name");
                                    MainGroupEmailBoxId = itemsemail.GetTypedColumnValue<Guid>(MailboxSyncSettings.Name);
                                    MainEmailBoxIdForReg = itemsemail.GetTypedColumnValue<Guid>("Id");
                                    MainGroupIdTemp = MainGroupId;
                                }
                                else if(email.ToUpper().Contains(EmailBoxName.ToUpper().Trim())
                                        || copies.ToUpper().Contains(EmailBoxName.ToUpper().Trim()))
                                {
                                    MainGroupIdByEmail = MainGroupId;
                                    MainGroupEmailBox = itemsemail.GetTypedColumnValue<string>("Name");
                                    MainGroupEmailBoxId = itemsemail.GetTypedColumnValue<Guid>(MailboxSyncSettings.Name);
                                    MainEmailBoxIdForReg = itemsemail.GetTypedColumnValue<Guid>("Id");
                                    MainGroupIdTemp = MainGroupId;
                                }
                            }
                        }

                        if (MainGroupIdTemp != Guid.Empty)
                        {
                            EntitySchemaQuery esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                            esq.PrimaryQueryColumn.IsAlwaysSelect = true;
                            esq.ChunkSize = 1;
                            var OlpTypeScheduleWorks = esq.AddColumn("OlpTypeScheduleWorks.Name");

                            esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", MainGroupIdTemp));

                            EntityCollection entityCollection = esq.GetEntityCollection(UserConnection);

                            //Идем в цикл если коллекция не пустая
                            if (entityCollection.IsNotEmpty()) 
                            {
                                foreach (var groupsshedule in entityCollection) 
                                {
                                    MainSheduleTypeByMail = groupsshedule.GetTypedColumnValue<string>(OlpTypeScheduleWorks.Name));
                                    return;
                                }
                            }
                            return;
                        }
                    }
                }
            }
            return;
            /**LEGACY**/
        }

        private void SetTravelParameter()
        {
            var title = activity.GetTypedColumnValue<string>("Title");
            var body = activity.GetTypedColumnValue<string>("Body");
            if (!string.IsNullOrEmpty(title))
            {
                themetravel = "";
                if (!string.IsNullOrEmpty(theme) && theme.ToUpper().Contains("TRAVEL-"))
                {

                    var regex = new Regex(@"(?<=TRAVEL-)\d+");

                    foreach (Match match in regex.Matches(theme))
                    {
                        themetravel = match.Value.ToString();
                        if(!string.IsNullOrEmpty(themetravel))
                        {
                            themetravel = "TRAVEL-" + themetravel;
                            break;
                        }
                    }
                }
            }

            if (string.IsNullOrEmpty(themetravel) && !string.IsNullOrEmpty(body) && body.ToUpper().Contains("TRAVEL-"))
            {

                var regex = new Regex(@"(?<=TRAVEL-)\d+");

                foreach (Match match in regex.Matches(body))
                {
                    themetravel = match.Value.ToString();
                    if(!string.IsNullOrEmpty(themetravel))
                    {
                        themetravel = "TRAVEL-" + themetravel;
                        break;
                    }
                }

            }

            string sql = @$"
                UPDATE 
                    \"Case\" 
                SET 
                    OlpReloThemeSibur = '{themetravel}',
                WHERE 
                    id = '{caseId}';
                
                UPDATE 
                    \"Case\"
                SET 
                    OlpTRAVELNumber = '{themetravel}_Закрыто/Отмененно',
                WHERE 
                    statusId = '{CASE_STATUS_CLOSED}' OR statusId = '{CASE_STATUS_CANCELED}' 
            ";

            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();

        }

        private void SetSiburParameter()
        {
            var title = activity.GetTypedColumnValue<string>("Title");
            var body = activity.GetTypedColumnValue<string>("Body");

            if (!string.IsNullOrEmpty(title))
            {
                if(string.IsNullOrEmpty(themetravel) && !string.IsNullOrEmpty(theme) && theme.ToUpper().Contains("ЗАЯВКА ПО РЕЛОКАЦИИ_")) 
                {

                    var regexurgent = new Regex(@"(?<=Заявка по релокации_)\[(.+)\]");
                    foreach (Match match in regexurgent.Matches(theme))
                    {
                        themetravel = match.Value.ToString();
                        if(!string.IsNullOrEmpty(themetravel)){
                            themetravel = "Заявка по релокации_" + themetravel;
                            break;
                        }
                    }
                }
            }

            string sql = @$" 
                UPDATE 
                    \"Case\" 
                SET 
                    OlpReloThemeSibur = '{themetravel}',
                WHERE 
                    id = '{caseId}';
                
                UPDATE 
                    \"Case\"
                SET 
                    OlpTRAVELNumber = '{themetravel}_Закрыто/Отмененно',
                WHERE 
                    statusId = '{CASE_STATUS_CLOSED}' OR statusId = '{CASE_STATUS_CANCELED}' 
                ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();    
        }

        private Guid GetAccountIdFromAccountCommunication()
        {
            string sql = @$"
                SELECT TOP 1 * FROM AccountCommunication
                WHERE 
                (
                 CommunicationType = '{COMMUNICATION_TYPE_EMAIL_DOMAIN}'
                 AND 
                 Number = '{domain}' 
                )
                OR
                (
                 CommunicationType = '{}'
                 AND 
                 Number = '{email}' 
                )
            ";

            CustomQuery query = new CustomQuery(UserConnection, sql);

            using (var db = UserConnection.EnsureDBConnection())
            {
                using (var reader = sql.ExecureReader(db))
                {
                    if (reader.Read())
                    {
                        return reader.GetColumnValue<Guid>("Account");
                    }
                }
            }
        }

        private void FetchAccountById(Guid accountId)
        {
            var account = new account(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { nameof(Account.Id), accountId },
            };

            if (account.FetchFromDB(conditions))
            {
                return account;
            }
        }

        private Account FetchAccountByEis(Guid accountId)
        {
            var account = new account(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { nameof(Account.OlpCode), accountId },
            };

            if (account.FetchFromDB(conditions))
            {
                return account;
            }
        }

        private void SetContactType(Guid contactId, Guid type)
        {
            var contactId = contact.GetTypedColumnValue<Guid>("Id");
            var companyId = account.GetTypedColumnValue<Guid>("Id");
            string sql = @$"
                UPDATE 
                    Contact
                SET 
                    Type = '{type}',
                    Account = '{companyId}'
                WHERE id = '{contactId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void SetSpamOnCase()
        {
            var contactId = contact.GetTypedColumnValue<Guid>("Id");
            string sql = @$"
                UPDATE 
                    Contact
                SET 
                    Type = '{CONTACT_TYPE_UNDEFINED_CLIENT_SPAM}', 
                    Account = '{OLP_AC_COMPANY_IS_NOT_DEFINED}' 
                WHERE
                    Id = '{contactId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void RefreshContact()
        {
            string OlpLnFnPat = eis.FirstName.English + " " + eis.MiddleName.English + " " + eis.LastName.English
            string sql = @$"
                UPDATE
                    Contact
                SET
                    Email = '{email}', 
                    Account = '{accountId}',
                    OlpBooleanAeroclubCheck = 1,
                    OlpSignVip = '{eis.isVip}',
                    OlpContactProfileConsLink = '{eis.ProfileLink}',
                    OlpLnFnPat = '{OlpLnFnPat}',
                    GivenName = '{eis.FirstName.Russian}',
                    MiddleName = '{eis.MiddleName.Russian}',
                    Surname = '{eis.Surname.Russian}',
                    OlpSignVipPlatf = '{eis.IsVipOnPlatform}',
                    OlpIsAuthorizedPerson = '{eis.IsAuthorized}',
                    OlpIsContactPerson = '{eis.IsContactPerson}',
                    Type = '{CONTACT_TYPE_CLIENT}',
                    OlpExternalContId = '{eis.Id}' 
                WHERE 
                    Id = '{contactId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void RefreshContactCompanyAndEmail()
        {
            var contactId contact.GetTypedColumnValue<Guid>("Id");
            var accountId = account.GetTypedColumnValue<Guid>("Id");
            string sql = @$"
                UPDATE 
                    Contact
                SET,
                    Email = '{email}',
                    Account = '{accountId}',
                    Type = '{CONTACT_TYPE_CLIENT}',
                WHERE 
                    Id = '{contactId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void RefreshContactTypeAndEmail()
        {
            var contactId contact.GetTypedColumnValue<Guid>("Id");
            string sql = @$"
                UPDATE 
                    Contact
                SET
                    Email = '{email}', 
                    Type = '{CONTACT_TYPE_CLIENT}',
                WHERE 
                    Id = '{contactId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void GetHoldingFromAccountBindedToEis()
        {
            var contactId contact.GetTypedColumnValue<Guid>("Id");
            string sql = @$"
                SELECT OlpHoldingId FROM Contact c 
                INNER JOIN Account a ON a.Id = c.AccountId
                WHERE c.Id = '{contactid}'
                ";

            CustomQuery query = new CustomQuery(UserConnection, sql);

            using (var db = UserConnection.EnsureDBConnection())
            {
                using (var reader = sql.ExecureReader(db))
                {
                    if (reader.Read())
                    {
                        return reader.GetColumnValue<Guid>("OlpHoldingId");
                    }
                }
            }
        }

        private void GetContactAfterRefreshing(Guid contactId)
        {
            var updatedContact = new Contact(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { nameof(Contact.Id), contactId}, 
            };

            if (updatedContact.FetchFromDB(conditions))
            {
                contact =  updatedContact;
            }
        }

        private void AddAccountToEmail()
        {
            string sql = @$"
                UPDATE 
                    Activity
                SET 
                    Account = '{clientCompanyId}', 
                WHERE 
                    Id = '{parentActivityId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private OlpServiceGroup GetServiceGroupBySelectedId()
        {
            var serviceGroup = new account(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                { nameof(Account.Id), selectedServiceGroupId},
            };

            if (serviceGroup.FetchFromDB(conditions))
            {
                return serviceGroup;
            }
        }

        // Найти основную ГО для контакта по компаниям и ВИП Платформа
        private void GetMainServiceGroup()
        {
            /**LEGACY**/

            //Считать признак поиска в ЕИС
            //Считать Ид. компании 
            Guid companyid = clientCompanyId;
            //Считать Ид. холдинга
            Guid holdingid = holding;
            //Считать ид. ящика из ГО по почте
            Guid MainEmailBoxIdForRegId = MainEmailBoxIdForReg;
            //Считать ВИП Платформа
            bool isvipplatform = clientVipPlatform;

            EntitySchemaQuery esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            esq.PrimaryQueryColumn.IsAlwaysSelect = true;	

            esq.AddColumn("[OlpGroupServiceAccount:OlpServiceGroupDetail:Id].OlpAccount"); //Ид.компании-холдинга из детали
            var OlpTypeServiceGroup = esq.AddColumn("OlpTypeGroupService.Name");
            var OlpTypeScheduleWorks = esq.AddColumn("OlpTypeScheduleWorks.Name");
            var orFilterGroup = new EntitySchemaQueryFilterCollection(esq, LogicalOperationStrict.Or);

            //Считать данные раздела ГО по основным группам/ВИП платформа
            if (isvipplatform == true)
            {
                esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpTypeGroupService.Name", "ВИП Платформа"));	
            }
            else
            {
                esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpTypeGroupService.Name", "Основная группа"));	

                if(MainEmailBoxIdForRegId != Guid.Empty)
                {
                    esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpSgEmail.Id", MainEmailBoxIdForRegId));
                }

                // Компания || Холдинг
                orFilterGroup.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpGroupServiceAccount:OlpServiceGroupDetail:Id].OlpAccount", companyid));
                orFilterGroup.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpGroupServiceAccount:OlpServiceGroupDetail:Id].OlpAccount", holdingid));
                esq.Filters.Add(orFilterGroup);
            }  

            var list = new CompositeObjectList<CompositeObject>();
            EntityCollection entityCollection = esq.GetEntityCollection(UserConnection);

            //Идем в цикл если коллекция не пустая
            if (entityCollection.IsNotEmpty()) 
            {
                foreach (var servicegroups in entityCollection) 
                {

                    var item = new CompositeObject();
                    var servicegroupid = servicegroups.GetTypedColumnValue<Guid>("Id");
                    var servicegroupttid = servicegroups.GetTypedColumnValue<string>(OlpTypeScheduleWorks.Name);
                    var servicegrouptype = servicegroups.GetTypedColumnValue<string>(OlpTypeServiceGroup.Name);

                    item["ProcessSchemaParameterServiceGroupId"] = servicegroupid;
                    item["ProcessSchemaParamServiceTimeTableId"] = servicegroupttid;
                    ServiceGroupForOrder = servicegroupid;

                    list.Add(item);
                }

                ProcessSchemaParameterServiceGroupCollection = list; 
            }        
            else
            { 
                //Дежурная группа
                ProcessSchemaParameterIsDutyGroup = true;
            }

            /**LEGACY**/
        }

        // Найти ГО основную по графику работы
        private void GetMainServiceGroupBasedOnTimetable()
        {
            /**LEGACY**/

            DateTime CurrentDayTime = DateTime.UtcNow.AddHours(3);
            var servicegrouplist = ProcessSchemaParameterServiceGroupCollection;
            var ServiceGroupIdTemp = System.Guid.Empty;

            foreach (var item in servicegrouplist)
            {
                Guid ServiceGroupId = item.TryGetValue<Guid>("ProcessSchemaParameterServiceGroupId"); 
                string ServiceTimeTableId = item.TryGetValue<Guid>("ProcessSchemaParameterServiceGroupId"); 
                
                if (ServiceTimeTableId == "Круглосуточно")
                {
                    ServiceGroupIdTemp = ServiceGroupId;
                    break;
                }

                //Приоритетная проверка по Праздничным-выходным дням
                EntitySchemaQuery EsqHoliday = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqHoliday.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqHoliday.ChunkSize = 1;
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));
                // Найти График в праздничные дни
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", CurrentDayTime));
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", CurrentDayTime));
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Выходной"));


                EntityCollection CollectionHoliday = EsqHoliday.GetEntityCollection(UserConnection);
                //Есть выходной-праздничный день перейти к следующей ГО
                if (CollectionHoliday.IsNotEmpty())
                {
                    continue;
                }

                //Приоритетная проверка по Праздничным-рабочим дням
                EntitySchemaQuery EsqHolidayWork = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqHolidayWork.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqHolidayWork.ChunkSize = 1;
                // Найти График в праздничные дни
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", CurrentDayTime));
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", CurrentDayTime));
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

                EntityCollection CollectionHolidayWork = EsqHolidayWork.GetEntityCollection(UserConnection);
                
                //Есть рабочий-праздничный день записать ГО и выйти из цикла
                if (CollectionHolidayWork.IsNotEmpty())
                {
                    foreach (var itemholidaywork in CollectionHolidayWork) 
                    {
                        ServiceGroupIdTemp = itemholidaywork.GetTypedColumnValue<Guid>("Id");
                    }
                    
                    if (ServiceGroupIdTemp != Guid.Empty)
                    {
                        break;
                    }
                }

                //Приоритетная проверка по Праздничным-рабочим дням - есть ли они на текущую дату?
                EntitySchemaQuery EsqHolidayWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqHolidayWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqHolidayWorkDate.ChunkSize = 1;
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow.AddHours(3).Date));
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Less, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow.AddHours(3).Date.AddDays(1)));
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

                EntityCollection CollectionHolidayWorkDate = EsqHolidayWorkDate.GetEntityCollection(UserConnection);
                //Есть рабочий-праздничный день записать ГО и выйти из цикла
                if (CollectionHolidayWorkDate.IsNotEmpty())
                {
                    continue;
                }

                //можно смотреть по стандартному графику
                EntitySchemaQuery EsqStandartWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqStandartWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqStandartWorkDate.ChunkSize = 2;

                var WeekDayColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code");
                var TimeFromColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeFrom");
                var TimeToColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeTo");
                var orFilterGroup = new EntitySchemaQueryFilterCollection(EsqStandartWorkDate, LogicalOperationStrict.Or);

                EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTypeDay.Name", "Рабочий")); // тип рабочего дня выходной/рабочий
                EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

                orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())); // день недели день недели -1
                orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.AddDays(-1).DayOfWeek.ToString())); // день недели день недели -1
                EsqStandartWorkDate.Filters.Add(orFilterGroup);

                EntityCollection CollectionStandartWorkDate = EsqStandartWorkDate.GetEntityCollection(UserConnection);

                if (CollectionStandartWorkDate.IsNotEmpty()) 
                {
                    foreach (var itemstandartwork in CollectionStandartWorkDate)
                    {

                        var WeekDay  = itemstandartwork.GetTypedColumnValue<string>(WeekDayColumn.Name);
                        var TimeFrom = itemstandartwork.GetTypedColumnValue<DateTime>(TimeFromColumn.Name);
                        var TimeTo   = itemstandartwork.GetTypedColumnValue<DateTime>(TimeToColumn.Name);

                        if (TimeFrom >= TimeTo) 
                        {
                            TimeTo = TimeTo.AddDays(1);
                        }
                        
                        if (WeekDay != DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())
                        {
                            TimeFrom = TimeFrom.AddDays(-1);TimeTo = TimeTo.AddDays(-1);
                        }

                        // Подходит ли основной график ГО
                        if (CurrentDayTime>TimeFrom && CurrentDayTime < TimeTo)
                        {
                            ServiceGroupIdTemp = itemstandartwork.GetTypedColumnValue<Guid>("Id");
                            break;
                        }
                    }
                    
                    if (ServiceGroupIdTemp != Guid.Empty)
                    {
                        break;
                    }
                }
            }

            if (ServiceGroupIdTemp == Guid.Empty){
                //дежурная группа 
                ProcessSchemaParameterIsDutyGroup = true;
            }
            else 
            {
                ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp);
            }

            return;
            
            /**LEGACY**/
        }

        // Найти ГО  основную по графику работы 1 этап
        private void GetMainServiceGroupBasedOnTimetableOlpFirstStage()
        {
            /**LEGACY**/
         
            DateTime CurrentDayTime = DateTime.UtcNow.AddHours(3);
            var servicegrouplist = ProcessSchemaParameterServiceGroupCollection;
            var ServiceGroupIdTemp = System.Guid.Empty;


            foreach (var item in servicegrouplist) 
            {
                string ServiceTimeTableId = item.TryGetValue<string>("ProcessSchemaParamServiceTimeTableId");
                Guid ServiceGroupId = item.TryGetValue<Guid>("ProcessSchemaParameterServiceGroupId");

                if (ServiceTimeTableId == "Круглосуточно")
                {
                    ServiceGroupIdTemp = ServiceGroupId;
                    break;
                }

                //Приоритетная проверка по Праздничным-выходным дням
                EntitySchemaQuery EsqHoliday = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqHoliday.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqHoliday.ChunkSize = 1;
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));
                // Найти График в праздничные дни
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
                EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Выходной"));


                EntityCollection CollectionHoliday = EsqHoliday.GetEntityCollection(UserConnection);
                //Есть выходной-праздничный день перейти к следующей ГО
                if (CollectionHoliday.IsNotEmpty())
                {
                    continue;
                }

                //Приоритетная проверка по Праздничным-рабочим дням
                EntitySchemaQuery EsqHolidayWork = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqHolidayWork.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqHolidayWork.ChunkSize = 1;
                // Найти График в праздничные дни
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
                EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

                EntityCollection CollectionHolidayWork = EsqHolidayWork.GetEntityCollection(UserConnection);
                //Есть рабочий-праздничный день записать ГО и выйти из цикла
                if (CollectionHolidayWork.IsNotEmpty())
                {
                    foreach (var itemholidaywork in CollectionHolidayWork) 
                    {
                        ServiceGroupIdTemp = itemholidaywork.GetTypedColumnValue<Guid>("Id");
                    }
                    
                    if (ServiceGroupIdTemp != Guid.Empty)
                    {
                        break;
                    }
                }

                //Приоритетная проверка по Праздничным-рабочим дням - есть ли они на текущую дату?
                EntitySchemaQuery EsqHolidayWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqHolidayWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqHolidayWorkDate.ChunkSize = 1;
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow.Date));
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Less, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow.Date.AddDays(1)));
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
                EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

                EntityCollection CollectionHolidayWorkDate = EsqHolidayWorkDate.GetEntityCollection(UserConnection);
                //Есть рабочий-праздничный день записать ГО и выйти из цикла
                if (CollectionHolidayWorkDate.IsNotEmpty())
                {
                    continue;
                }

                //можно смотреть по стандартному графику
                EntitySchemaQuery EsqStandartWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                EsqStandartWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqStandartWorkDate.ChunkSize = 2;

                var WeekDayColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code");
                var TimeFromColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeFrom");
                var TimeToColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeTo");
                var orFilterGroup = new EntitySchemaQueryFilterCollection(EsqStandartWorkDate, LogicalOperationStrict.Or);

                EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTypeDay.Name", "Рабочий")); // тип рабочего дня выходной/рабочий
                EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

                orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())); // день недели день недели -1
                orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.AddDays(-1).DayOfWeek.ToString())); // день недели день недели -1
                EsqStandartWorkDate.Filters.Add(orFilterGroup);

                EntityCollection CollectionStandartWorkDate = EsqStandartWorkDate.GetEntityCollection(UserConnection);

                if (CollectionStandartWorkDate.IsNotEmpty()) 
                {
                    foreach (var itemstandartwork in CollectionStandartWorkDate)
                    {

                        var WeekDay  = itemstandartwork.GetTypedColumnValue<string>(WeekDayColumn.Name);
                        var TimeFrom = itemstandartwork.GetTypedColumnValue<DateTime>(TimeFromColumn.Name);
                        var TimeTo   = itemstandartwork.GetTypedColumnValue<DateTime>(TimeToColumn.Name);

                        if (TimeFrom >= TimeTo) {TimeTo = TimeTo.AddDays(1);}
                        if (WeekDay != DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString()){TimeFrom = TimeFrom.AddDays(-1);TimeTo = TimeTo.AddDays(-1);}

                        // Подходит ли основной график ГО
                        if (CurrentDayTime>TimeFrom && CurrentDayTime < TimeTo)
                        {
                            ServiceGroupIdTemp = itemstandartwork.GetTypedColumnValue<Guid>("Id");
                            break;
                        }
                    }
                    if (ServiceGroupIdTemp != Guid.Empty)
                    {
                        break;
                    }
                }
            }

            if (ServiceGroupIdTemp == Guid.Empty)
            {
                //дежурная группа 
                ProcessSchemaParameterIsDutyGroup = true;
            }
            else 
            {
                ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                ProcessSchemaParameterIsDutyGroup = false;
            }

            return true;
         
            /**LEGACY**/
        }

        // Найти осн из почты по графику
        private void GetMainServiceGroupFromMainBasedOnTimeTable()
        {
            DateTime CurrentDayTime = DateTime.UtcNow.AddHours(3);

            Guid ServiceGroupId = MainGroupIdByEmail;
            string sheduletype = MainSheduleTypeByMail;

            Guid ServiceGroupIdTemp = System.Guid.Empty;

            //можно смотреть по стандартному графику
            EntitySchemaQuery EsqStdWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqStdWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqStdWorkDate.ChunkSize = 1;

            var StdTimeFromColumn = EsqStdWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeFrom");
            var StdTimeToColumn = EsqStdWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeTo");

            EsqStdWorkDate.Filters.Add(EsqStdWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTypeDay.Name", "Рабочий")); // тип рабочего дня выходной/рабочий
            EsqStdWorkDate.Filters.Add(EsqStdWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", "Monday"));
            EsqStdWorkDate.Filters.Add(EsqStdWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionStdWorkDate = EsqStdWorkDate.GetEntityCollection(UserConnection);

            if (CollectionStdWorkDate.IsNotEmpty())
            {
                ProcessSchemaParameter2 = 1;
                foreach (var itemstdwork in CollectionStdWorkDate)
                {

                    var StdTimeFrom = itemstdwork.GetTypedColumnValue<DateTime>(StdTimeFromColumn.Name);
                    var StdTimeTo   = itemstdwork.GetTypedColumnValue<DateTime>(StdTimeToColumn.Name);
                    ProcessSchemaParameter2 = 1;

                    if(StdTimeFrom != null && StdTimeTo  != null)
                    {
                        DateFromForReply = StdTimeFrom.ToString("HH:mm");
                        DateToForReply = StdTimeTo.ToString("HH:mm");
                    }
                    else
                    {
                        DateFromForReply = "09:00";
                        DateToForReply = "19:00";
                    }

                    break;
                }
            }
            else
            {
                DateFromForReply = "09:00";
                DateToForReply = "19:00";
            }


            if (sheduletype == "Круглосуточно")
            {
                ServiceGroupIdTemp = ServiceGroupId;
                ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                ProcessSchemaParameterIsDutyGroup = false;
                return true;
            }

            //Приоритетная проверка по Праздничным-выходным дням
            EntitySchemaQuery EsqHoliday = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHoliday.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHoliday.ChunkSize = 1;
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));
            // Найти График в праздничные дни
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Выходной"));

            EntityCollection CollectionHoliday = EsqHoliday.GetEntityCollection(UserConnection);
            //Есть выходной-праздничный день перейти к следующей ГО
            if (CollectionHoliday.IsNotEmpty()){
                ProcessSchemaParamServiceGroupId = ServiceGroupId;
                ProcessSchemaParameterIsDutyGroup = true;
                ServiceGroupForOrder = ServiceGroupId;
                return true;}

            //Приоритетная проверка по Праздничным-рабочим дням
            EntitySchemaQuery EsqHolidayWork = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHolidayWork.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHolidayWork.ChunkSize = 1;
            // Найти График в праздничные дни
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionHolidayWork = EsqHolidayWork.GetEntityCollection(UserConnection);
            //Есть рабочий-праздничный день записать ГО и выйти из цикла
            if (CollectionHolidayWork.IsNotEmpty())
            {
                foreach (var itemholidaywork in CollectionHolidayWork) 
                {
                    ServiceGroupIdTemp = itemholidaywork.GetTypedColumnValue<Guid>("Id");
                }

                if (ServiceGroupIdTemp != Guid.Empty)
                {
                    ProcessSchemaParamServiceGroupId =  ServiceGroupIdTemp;
                    ProcessSchemaParameterIsDutyGroup  = false;
                    return true;
                }
            }

            //Приоритетная проверка по Праздничным-рабочим дням - есть ли они на текущую дату?
            EntitySchemaQuery EsqHolidayWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHolidayWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHolidayWorkDate.ChunkSize = 1;
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow.Date));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Less, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow.Date.AddDays(1)));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionHolidayWorkDate = EsqHolidayWorkDate.GetEntityCollection(UserConnection);
            //Есть рабочий-праздничный день записать ГО и выйти из цикла
            if (CollectionHolidayWorkDate.IsNotEmpty())
            {
                ProcessSchemaParamServiceGroupId = ServiceGroupId;
                ProcessSchemaParameterIsDutyGroup = true;
                ServiceGroupForOrder = ServiceGroupId;
                return true;
            }

            //можно смотреть по стандартному графику
            EntitySchemaQuery EsqStandartWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqStandartWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqStandartWorkDate.ChunkSize = 2;

            var WeekDayColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code");
            var TimeFromColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeFrom");
            var TimeToColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeTo");
            var orFilterGroup = new EntitySchemaQueryFilterCollection(EsqStandartWorkDate, LogicalOperationStrict.Or);

            EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTypeDay.Name", "Рабочий")); // тип рабочего дня выходной/рабочий
            EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())); // день недели день недели -1
            orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.AddDays(-1).DayOfWeek.ToString())); // день недели день недели -1
            EsqStandartWorkDate.Filters.Add(orFilterGroup);

            EntityCollection CollectionStandartWorkDate = EsqStandartWorkDate.GetEntityCollection(UserConnection);

            if (CollectionStandartWorkDate.IsNotEmpty()) 
            {
                foreach (var itemstandartwork in CollectionStandartWorkDate)
                {

                    var WeekDay  = itemstandartwork.GetTypedColumnValue<string>(WeekDayColumn.Name);
                    var TimeFrom = itemstandartwork.GetTypedColumnValue<DateTime>(TimeFromColumn.Name);
                    var TimeTo   = itemstandartwork.GetTypedColumnValue<DateTime>(TimeToColumn.Name);

                    if (TimeFrom >= TimeTo) 
                    {
                        TimeTo = TimeTo.AddDays(1);
                    }

                    if (WeekDay != DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())
                    {
                        TimeFrom = TimeFrom.AddDays(-1);TimeTo = TimeTo.AddDays(-1);
                    }

                    // Подходит ли основной график ГО
                    if (CurrentDayTime>TimeFrom && CurrentDayTime < TimeTo)
                    {
                        ServiceGroupIdTemp = itemstandartwork.GetTypedColumnValue<Guid>("Id");
                        break;
                    }
                }
                
                if (ServiceGroupIdTemp != Guid.Empty)
                {
                    ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                    ProcessSchemaParameterIsDutyGroup = false;
                }
            }


            if (ServiceGroupIdTemp == Guid.Empty)
            {
                //дежурная группа
                ProcessSchemaParamServiceGroupId = ServiceGroupId;
                ProcessSchemaParameterIsDutyGroup = true;
                ServiceGroupForOrder = ServiceGroupId;
            }

            return true;

            /**LEGACY**/
        }

        // Найти ГО дежурную из кому/копии по графику работы
        private void GetExtraServiceGroupFromFromAndCopyBaseOnTimetable()
        {
            /**LEGACY**/
            
            DateTime CurrentDayTime = DateTime.UtcNow.AddHours(3);
            var k=0;			
            Guid ServiceGroupId = ExtraGroupIdByEmail;
            string sheduletype = ExtraSheduleTypeByMail;
            Guid ServiceGroupIdTemp = System.Guid.Empty;

            if (sheduletype == "Круглосуточно")
            {
                ServiceGroupIdTemp = ServiceGroupId;
                ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                ProcessSchemaParameterIsDutyGroup = false;
                return true;
            }

            //Приоритетная проверка по Праздничным-выходным дням
            EntitySchemaQuery EsqHoliday = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHoliday.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHoliday.ChunkSize = 1;
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));
            // Найти График в праздничные дни
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Выходной"));


            EntityCollection CollectionHoliday = EsqHoliday.GetEntityCollection(UserConnection);
            //Есть выходной-праздничный день перейти к следующей ГО
            if (CollectionHoliday.IsNotEmpty())
            {
                ProcessSchemaParameterIsDutyGroup = true;
                return true;
            }

            //Приоритетная проверка по Праздничным-рабочим дням
            EntitySchemaQuery EsqHolidayWork = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHolidayWork.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHolidayWork.ChunkSize = 1;
            // Найти График в праздничные дни
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionHolidayWork = EsqHolidayWork.GetEntityCollection(UserConnection);
            //Есть рабочий-праздничный день записать ГО и выйти из цикла
            if (CollectionHolidayWork.IsNotEmpty())
            {
                foreach (var itemholidaywork in CollectionHolidayWork) 
                {
                    ServiceGroupIdTemp = itemholidaywork.GetTypedColumnValue<Guid>("Id");
                }
                if (ServiceGroupIdTemp != Guid.Empty)
                {
                    ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                    ProcessSchemaParameterIsDutyGroup = false;
                    return true;
                }
            }

            //Приоритетная проверка по Праздничным-рабочим дням - есть ли они на текущую дату?
            EntitySchemaQuery EsqHolidayWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHolidayWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHolidayWorkDate.ChunkSize = 1;
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow.Date));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Less, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow.Date.AddDays(1)));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionHolidayWorkDate = EsqHolidayWorkDate.GetEntityCollection(UserConnection);
            //Есть рабочий-праздничный день записать ГО и выйти из цикла
            if (CollectionHolidayWorkDate.IsNotEmpty())
            {
                ProcessSchemaParameterIsDutyGroup = true;
                return true;
            }

            //можно смотреть по стандартному графику
            EntitySchemaQuery EsqStandartWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqStandartWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqStandartWorkDate.ChunkSize = 2;

            var WeekDayColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code");
            var TimeFromColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeFrom");
            var TimeToColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeTo");
            var orFilterGroup = new EntitySchemaQueryFilterCollection(EsqStandartWorkDate, LogicalOperationStrict.Or);

            EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTypeDay.Name", "Рабочий")); // тип рабочего дня выходной/рабочий
            EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())); // день недели день недели -1
            orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.AddDays(-1).DayOfWeek.ToString())); // день недели день недели -1
            EsqStandartWorkDate.Filters.Add(orFilterGroup);

            EntityCollection CollectionStandartWorkDate = EsqStandartWorkDate.GetEntityCollection(UserConnection);

            if (CollectionStandartWorkDate.IsNotEmpty())
            {
                foreach (var itemstandartwork in CollectionStandartWorkDate)
                {
                    k=k+1;
                    var WeekDay  = itemstandartwork.GetTypedColumnValue<string>(WeekDayColumn.Name);
                    var TimeFrom = itemstandartwork.GetTypedColumnValue<DateTime>(TimeFromColumn.Name);
                    var TimeTo   = itemstandartwork.GetTypedColumnValue<DateTime>(TimeToColumn.Name);

                    if (TimeFrom >= TimeTo) 
                    {
                        TimeTo = TimeTo.AddDays(1);
                    }
                    
                    if (WeekDay != DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())
                    {
                        TimeFrom = TimeFrom.AddDays(-1);TimeTo = TimeTo.AddDays(-1);
                    }

                    // Подходит ли основной график ГО
                    if (CurrentDayTime>TimeFrom && CurrentDayTime < TimeTo)
                    {
                        ServiceGroupIdTemp = itemstandartwork.GetTypedColumnValue<Guid>("Id");
                        break;
                    }
                }
                if (ServiceGroupIdTemp != Guid.Empty)
                {
                    ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                    ProcessSchemaParameterIsDutyGroup = false;
                }
            }


            if (ServiceGroupIdTemp == Guid.Empty)
            {
                //дежурная группа 
                ProcessSchemaParameterIsDutyGroup = true;
            }
            ProcessSchemaParameter3 = k;
            return true;

            /**LEGACY**/
        }

        // Найти ГО дежурную по графику работы
        private void GetExtraServiceGroupBaseOnTimetable()
        {
            DateTime CurrentDayTime = DateTime.UtcNow.AddHours(3);

            Guid ServiceGroupId = OLP_DUTY_SERVICE_GROUP; // TODO Вроде это
            string sheduletype = "";

            if (ServiceGroupId != Guid.Empty)
            {
                EntitySchemaQuery esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
                esq.PrimaryQueryColumn.IsAlwaysSelect = true;
                esq.ChunkSize = 1;
                var OlpTypeScheduleWorks = esq.AddColumn("OlpTypeScheduleWorks.Name");
                esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));
                EntityCollection entityCollection = esq.GetEntityCollection(UserConnection);
                //Идем в цикл если коллекция не пустая
                if (entityCollection.IsNotEmpty()) 
                {
                    foreach (var groupsshedule in entityCollection) 
                    {
                        sheduletype = groupsshedule.GetTypedColumnValue<string>(OlpTypeScheduleWorks.Name);
                        break;
                    }
                }
            }

            Guid ServiceGroupIdTemp = System.Guid.Empty;

            if (sheduletype == "Круглосуточно")
            {
                ServiceGroupIdTemp = ServiceGroupId;
                ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                ProcessSchemaParameterIsDutyGroup = false;
                return true;
            }

            //Приоритетная проверка по Праздничным-выходным дням
            EntitySchemaQuery EsqHoliday = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHoliday.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHoliday.ChunkSize = 1;
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));
            // Найти График в праздничные дни
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
            EsqHoliday.Filters.Add(EsqHoliday.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Выходной"));


            EntityCollection CollectionHoliday = EsqHoliday.GetEntityCollection(UserConnection);
            //Есть выходной-праздничный день перейти к следующей ГО
            if (CollectionHoliday.IsNotEmpty())
            {
                ProcessSchemaParameterIsDutyGroup = true;
                return true;
            }

            //Приоритетная проверка по Праздничным-рабочим дням
            EntitySchemaQuery EsqHolidayWork = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHolidayWork.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHolidayWork.ChunkSize = 1;
            // Найти График в праздничные дни
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.LessOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
            EsqHolidayWork.Filters.Add(EsqHolidayWork.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionHolidayWork = EsqHolidayWork.GetEntityCollection(UserConnection);
            //Есть рабочий-праздничный день записать ГО и выйти из цикла
            if (CollectionHolidayWork.IsNotEmpty())
            {
                foreach (var itemholidaywork in CollectionHolidayWork) 
                {
                    ServiceGroupIdTemp = itemholidaywork.GetTypedColumnValue<Guid>("Id");
                }

                if (ServiceGroupIdTemp != Guid.Empty)
                {
                    ProcessSchemaParamServiceGroupId = ServiceGroupIdTemp;
                    ProcessSchemaParameterIsDutyGroup = false;
                    return true;
                }
            }

            //Приоритетная проверка по Праздничным-рабочим дням - есть ли они на текущую дату?
            EntitySchemaQuery EsqHolidayWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqHolidayWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqHolidayWorkDate.ChunkSize = 1;
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.GreaterOrEqual, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeFrom", DateTime.UtcNow.Date));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Less, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpDatetimeTo", DateTime.UtcNow.Date.AddDays(1)));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpHolidayException:OlpServiceGroupHolidaysDetail:Id].OlpTypeDay.Name", "Рабочий"));
            EsqHolidayWorkDate.Filters.Add(EsqHolidayWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            EntityCollection CollectionHolidayWorkDate = EsqHolidayWorkDate.GetEntityCollection(UserConnection);
            //Есть рабочий-праздничный день записать ГО и выйти из цикла
            if (CollectionHolidayWorkDate.IsNotEmpty())
            {
                ProcessSchemaParameterIsDutyGroup = true;
                return true;
            }

            //можно смотреть по стандартному графику
            EntitySchemaQuery EsqStandartWorkDate = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            EsqStandartWorkDate.PrimaryQueryColumn.IsAlwaysSelect = true;
            EsqStandartWorkDate.ChunkSize = 2;

            var WeekDayColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code");
            var TimeFromColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeFrom");
            var TimeToColumn = EsqStandartWorkDate.AddColumn("[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTimeTo");
            var orFilterGroup = new EntitySchemaQueryFilterCollection(EsqStandartWorkDate, LogicalOperationStrict.Or);

            EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpTypeDay.Name", "Рабочий")); // тип рабочего дня выходной/рабочий
            EsqStandartWorkDate.Filters.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "Id", ServiceGroupId));

            orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())); // день недели день недели -1
            orFilterGroup.Add(EsqStandartWorkDate.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpServiceGroupWork:OlpServiceGroupWorkSched:Id].OlpWeekDay.Code", DateTime.UtcNow.AddHours(3).Date.AddDays(-1).DayOfWeek.ToString())); // день недели день недели -1
            EsqStandartWorkDate.Filters.Add(orFilterGroup);

            EntityCollection CollectionStandartWorkDate = EsqStandartWorkDate.GetEntityCollection(UserConnection);

            if (CollectionStandartWorkDate.IsNotEmpty()) 
            {
                foreach (var itemstandartwork in CollectionStandartWorkDate)
                {

                    var WeekDay  = itemstandartwork.GetTypedColumnValue<string>(WeekDayColumn.Name);
                    var TimeFrom = itemstandartwork.GetTypedColumnValue<DateTime>(TimeFromColumn.Name);
                    var TimeTo   = itemstandartwork.GetTypedColumnValue<DateTime>(TimeToColumn.Name);

                    if (TimeFrom >= TimeTo) 
                    {
                        TimeTo = TimeTo.AddDays(1);
                    }
                    
                    if (WeekDay != DateTime.UtcNow.AddHours(3).Date.DayOfWeek.ToString())
                    {
                        TimeFrom = TimeFrom.AddDays(-1);TimeTo = TimeTo.AddDays(-1);
                    }

                    // Подходит ли основной график ГО
                    if (CurrentDayTime>TimeFrom && CurrentDayTime < TimeTo)
                    {
                        ServiceGroupIdTemp = itemstandartwork.GetTypedColumnValue<Guid>("Id");
                        break;
                    }
                }
                if (ServiceGroupIdTemp != Guid.Empty)
                {
                    ProcessSchemaParameterIsDutyGroup = false;
                }
            }


            if (ServiceGroupIdTemp == Guid.Empty)
            {
                //дежурная группа 
                ProcessSchemaParameterIsDutyGroup = true;
            }

            return true;
        }

        private void SetFirstLineSupport()
        {
            string sql = @$"
                UPDATE 
                   \"Case\"
                SET 
                    OlpGroupServices = '{selectedServiceGroupId}',
                    OlpSupportLine = '{OLP_OR_FIRST_LINE_SUPPORT}',
                    OlpImportant = '{importancy}',
                    OlpUrgency = '{CASE_URGENCY_TYPE_NOT_URGENT}',
                    OlpIsAuthorVIP = '{clientVip}',
                    Account = '{clientCompanyId}',
                    Category = '{caseCategory}',
                    OlpServiceGroupForOrder = '{OlpServiceGroupForOrder}', 
                WHERE 
                    Id = '{caseId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();

        }

        private void SetSecondLineSupport()
        {
            string sql = @$" 
                UPDATE 
                   \"Case\"
                SET 
                    OlpGroupServices = '{selectedServiceGroupId}',
                    OlpSupportLine = '{OLP_OR_SECOND_LINE_SUPPORT}',
                    OlpImportant = '{importancy}',
                    OlpUrgency = '{CASE_URGENCY_TYPE_NOT_URGENT}',
                    OlpIsAuthorVIP = '{clientVip}',
                    Account = '{clientCompanyId}',
                    Category = '{caseCategory}',
                    OlpServiceGroupForOrder = '{OlpServiceGroupForOrder}', 
                WHERE
                    Id = '{caseId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void SetThirdLineSupport()
        {
            string sql = @$"
                UPDATE 
                   \"Case\"
                SET 
                    OlpGroupServices = '{selectedServiceGroupId}',
                    OlpSupportLine = '{OLP_OR_THIRD_LINE_SUPPORT}',
                    OlpImportant = '{CASE_IMPORTANCY_IMPOTANT}',
                    OlpUrgency = '{CASE_URGENCY_TYPE_NOT_URGENT}',
                    OlpIsAuthorVIP = '{clientVip}',
                    Account = '{clientCompanyId}',
                    Category = '{caseCategory}',
                    OlpServiceGroupForOrder = '{OlpServiceGroupForOrder}', 
                WHERE 
                    Id = '{caseId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
 
        }

        private void SendBookAutoreply()
        {
            // TODO
        }

        private void SetAutonotification()
        {
            string sql = @$"
                UPDATE
                    Activity
                SET 
                    IsAutoSubmitted = '{true}',
                WHERE 
                    id = '{parentActivityId}' // TODO Is Parent activity Id needed?
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void RefreshEmailsAndPhones()
        {
            /**LEGACY**/

            // обновление/добавление почты
            var emailList = eis.Emails; 
            var IdContact = contactId;

            foreach (var item in emailList) 
            {

                NameEmail = item.Address;

                if (string.IsNullOrEmpty(NameEmail)) { continue; }	// если нет почты то идти на следующий


                // Существует email?
                EntitySchemaQuery EsqContact = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "ContactCommunication");
                EsqContact.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqContact.ChunkSize = 1;
                EsqContact.Filters.Add(EsqContact.CreateFilterWithParameters(FilterComparisonType.Equal, "Contact", IdContact));
                EsqContact.Filters.Add(EsqContact.CreateFilterWithParameters(FilterComparisonType.Equal, "Number", NameEmail));
                EsqContact.Filters.Add(EsqContact.CreateFilterWithParameters(FilterComparisonType.Equal, "CommunicationType", Guid.Parse("ee1c85c3-cfcb-df11-9b2a-001d60e938c6")));
                EntityCollection CollectionEmail = EsqContact.GetEntityCollection(UserConnection);

                if (CollectionEmail.IsEmpty())
                {
                    // continue;
                    // Добавление почты контакту
                    var emailContact = UserConnection.EntitySchemaManager.GetInstanceByName("ContactCommunication");
                    var entityemailContact = emailContact.CreateEntity(UserConnection);

                    entityemailContact.UseAdminRights = false;
                    entityemailContact.SetDefColumnValues()
                    entityemailContact.SetColumnValue("ContactId", IdContact );
                    entityemailContact.SetColumnValue("Number", NameEmail);
                    entityemailContact.SetColumnValue("CommunicationTypeId", Guid.Parse("ee1c85c3-cfcb-df11-9b2a-001d60e938c6"));
                    entityemailContact.Save();
                }

            }

            // обновление/добавление телефона
            var listPhone = eis.OlpPhones_Out);
            var IdContact1 = ContactIdForEmailAndPhone;

            foreach (var item1 in listPhone) 
            {
                NamePhone = item1.OlpPNumber_Out;
                Kind = item1.OlpPKind_Out;

                if (string.IsNullOrEmpty(NamePhone)) { continue; }	// если нет телефона то идти на следующий

                var typeIdPhone = Guid.Empty;

                // найти тип телефона
                if (Kind == "mobile") { typeIdPhone = Guid.Parse("d4a2dc80-30ca-df11-9b2a-001d60e938c6"); }
                if (Kind == "work") { typeIdPhone = Guid.Parse("3dddb3cc-53ee-49c4-a71f-e9e257f59e49"); }
                if (Kind == "home") { typeIdPhone = Guid.Parse("0da6a26b-d7bc-df11-b00f-001d60e938c6"); } 
                if (string.IsNullOrEmpty(Kind)) { typeIdPhone = Guid.Parse("21c0d693-9a52-43fa-b7f1-c6d8b53975d4"); }

                // Существует телефон?
                EntitySchemaQuery EsqContactPhone = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "ContactCommunication");
                EsqContactPhone.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqContactPhone.ChunkSize = 1;
                EsqContactPhone.Filters.Add(EsqContactPhone.CreateFilterWithParameters(FilterComparisonType.Equal, "Contact", IdContact1));
                EsqContactPhone.Filters.Add(EsqContactPhone.CreateFilterWithParameters(FilterComparisonType.Equal, "Number", NamePhone));
                EsqContactPhone.Filters.Add(EsqContactPhone.CreateFilterWithParameters(FilterComparisonType.Equal, "CommunicationType", typeIdPhone));
                EntityCollection CollectionPhone = EsqContactPhone.GetEntityCollection(UserConnection);

                if (CollectionPhone.IsEmpty())
                {
                    // Добавление почты контакту
                    var phoneContact = UserConnection.EntitySchemaManager.GetInstanceByName("ContactCommunication");
                    var entityphoneContact = phoneContact.CreateEntity(UserConnection);

                    entityphoneContact.UseAdminRights = false;
                    entityphoneContact.SetDefColumnValues();
                    entityphoneContact.SetColumnValue("ContactId", IdContact1 );
                    entityphoneContact.SetColumnValue("Number", NamePhone);
                    entityphoneContact.SetColumnValue("CommunicationTypeId", typeIdPhone);
                    entityphoneContact.Save();
                }

            }
            return;
            /**LEGACY**/
        }

        // Найти основную ГО для контакта по компаниям
        private void GetMainServiceGroupForContactBasedOnCompany()
        {
            /**LEGACY**/
            
            //Считать признак поиска в ЕИС
            //Считать Ид. компании 
            Guid companyid = ProcessSchemaParamCompanyFoundId; // TODO
            //Считать Ид. холдинга
            Guid holdingid = ProcessSchemaParamHoldingFoundId; // TODO

            //Считать ВИП Платформа
            // Get<bool>("ProcessSchemaParamClientFoundIsVIPPl"); TODO
            bool isvipplatform = clientVipPlatform; 

            EntitySchemaQuery esq = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServiceGroup");
            esq.PrimaryQueryColumn.IsAlwaysSelect = true;
            esq.ChunkSize = 1;
            //esq.AddColumn("OlpSgEmail.Name"); //Почтовый ящик string
            esq.AddColumn("[OlpGroupServiceAccount:OlpServiceGroupDetail:Id].OlpAccount"); //Ид.компании-холдинга из детали
            var orFilterGroup = new EntitySchemaQueryFilterCollection(esq, LogicalOperationStrict.Or);

            //Считать данные раздела ГО по основным группам/ВИП платформа
            if (isvipplatform == true)
            {
                esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpTypeGroupService.Name", "ВИП Платформа"));	
            }
            
            else
            {
                esq.Filters.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpTypeGroupService.Name", "Основная группа"));	

                // Компания || Холдинг
                orFilterGroup.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpGroupServiceAccount:OlpServiceGroupDetail:Id].OlpAccount", companyid));
                orFilterGroup.Add(esq.CreateFilterWithParameters(FilterComparisonType.Equal, "[OlpGroupServiceAccount:OlpServiceGroupDetail:Id].OlpAccount", holdingid));
                esq.Filters.Add(orFilterGroup);
            }  

            EntityCollection entityCollection = esq.GetEntityCollection(UserConnection);

            //Идем в цикл если коллекция не пустая
            if (entityCollection.IsNotEmpty()) 
            {
                foreach (var servicegroups in entityCollection) 
                {
                    var servicegroupid = servicegroups.GetTypedColumnValue<Guid>("Id");
                    ProcessSchemaParamServiceGroupId = servicegroupid;
                    ProcessSchemaParameterIsDutyGroup = false;
                }
            }       
            else
            { 
                //Дежурная группа
                ProcessSchemaParameterIsDutyGroup = true;

            }

            return true;

            /**LEGACY**/
        }

        private void GetLoadingCheck()
        {
            sql = @$"
                SELECT 
                    BooleanValue 
                FROM 
                    SysSettingsValue 
                WHERE 
                    SysSettingsId = (
                        SELECT id FROM SysSettings WHERE Code LIKE 'OLPLoadingCheck'
                        )
                ";

            CustomQuery query = new CustomQuery(UserConnection, sql);

            using (var db = UserConnection.EnsureDBConnection())
            {
                using (var reader = sql.ExecureReader(db))
                {
                    if (reader.Read())
                    {
                        return reader.GetColumnValue<bool>("BooleanValue");
                    }
                }
            }
        }

        private void CollectServicesForInsertion()
        {
            var servicesList = eis.OlpServices_Out; // TODO

            bool nagruzka = LoadingCheck;
            int  nagruzkacount = 0;

            var list = new CompositeObjectList<CompositeObject>();

            foreach (var item in servicesList) {
                //Если 
                if (item.TryGetValue<string>("OlpSONumber_Out", out string OrderNumber)){}
                if (item.TryGetValue<string>("OlpSSNumber1_Out", out string ServiceNumber)){}
                if (item.TryGetValue<string>("OlpSTNumber_Out", out string TripNumber)){}

                string OrderNumber = item.TryGetValue<string>("OlpSONumber_Out");
                string ServiceNumber = item.TryGetValue<string>("OlpSSNumber1_Out");
                string TripNumber = item.TryGetValue<string>("OlpSTNumber_Out");

                //Если передается пустота - выходим
                if (ServiceNumber == "0") 
                {
                    continue;
                }

                //Проверка существует ли услуга в системе
                EntitySchemaQuery EsqServise = new EntitySchemaQuery(UserConnection.EntitySchemaManager, "OlpServices");
                EsqServise.PrimaryQueryColumn.IsAlwaysSelect = true;
                EsqServise.ChunkSize = 1;
                EsqServise.Filters.Add(EsqServise.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpExternalOrderNumber", OrderNumber));
                EsqServise.Filters.Add(EsqServise.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpExternalServiceId", ServiceNumber));
                EsqServise.Filters.Add(EsqServise.CreateFilterWithParameters(FilterComparisonType.Equal, "OlpExternalTripId", TripNumber));

                EntityCollection CollectionEsqServise = EsqServise.GetEntityCollection(UserConnection);

                if (CollectionEsqServise.IsNotEmpty() && nagruzka == true && nagruzkacount <= 5)
                {
                    var itemo = new CompositeObject();
                    itemo["ServOrderNumber"] = OrderNumber;
                    itemo["ServiceNumb"] = ServiceNumber;
                    itemo["ProcessSchemaParamExtTripId"] = TripNumber;
                    list.Add(itemo);
                    OrderNumber = "";
                    TripNumber = "";
                    ServiceNumber = "";

                    nagruzkacount = nagruzkacount + 1;
                }


                else if (CollectionEsqServise.IsNotEmpty() && (nagruzka == false || nagruzkacount > 5))
                {
                    continue;
                }

                else
                {
                    var itemo = new CompositeObject();
                    itemo["ServOrderNumber"] = OrderNumber;
                    itemo["ServiceNumb"] = ServiceNumber;
                    itemo["ProcessSchemaParamExtTripId"] = TripNumber;
                    list.Add(itemo);
                    OrderNumber = "";
                    TripNumber = "";
                    ServiceNumber = "";
                }
            }

            ServicesToAdd = list;

            ProcessSchemaParamTime = DateTime.Now.ToString("yyyyMMddHHmmssfffffff");

            return true;
        }
        
        private bool SendEisRequest()
        {
            string id = "";
            string phone = "";
    
            // Email here is 'private string email' from activity 
            string url = $"http://services.aeroclub.int/bpmintegration/profiles/get-info?Id={id}&Email={email}&Phone={phone}"; 
            
            try
            {
                using (WebClient client = new WebClient())
                {
                    client.Encoding = Encoding.UTF8;

                    string responseBody = client.DownloadString(url);

                    using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(responseBody)))
                    {
                        var serializer = new DataContractJsonSerializer(typeof(Profile));
                        eis = (Profile)serializer.ReadObject(ms);
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                return false;
            }
        }
    }

    public class Profile
    {
        public int Id { get; set; }
        
        public Name FirstName { get; set; }
        
        public Name LastName { get; set; }
        
        public Name MiddleName { get; set; }
        
        public Company Company { get; set; }
        
        public DateTime DateOfBirth { get; set; }
        
        public string Gender { get; set; }
        
        public bool IsAuthorized { get; set; }
        
        public bool IsVip { get; set; }
        
        public bool IsContactPerson { get; set; }
        
        public bool IsVipOnPlatform { get; set; }
        
        public string EmployeeNumber { get; set; }
        
        public string EmployeeLevel { get; set; }
        
        public string ProfileLink { get; set; }
        
        public List<CustomProperty> CustomProperties { get; set; }
        
        public List<Email> Emails { get; set; }
        
        public List<Phone> Phones { get; set; }
    }

    public class Name
    {
        public string English { get; set; }
        public string Russian { get; set; }
    }

    public class Company
    {
        public int Id { get; set; }
        public Name Name { get; set; }
    }

    public class CustomProperty
    {
        public Property Property { get; set; }
        public Value Value { get; set; }
    }

    public class Property
    {
        public string English { get; set; }
        public string Russian { get; set; }
    }

    public class Value
    {
        public string English { get; set; }
        public string Russian { get; set; }
    }

    public class Email
    {
        public string Kind { get; set; }
        public string Address { get; set; }
    }

    public class Phone
    {
        public string Kind { get; set; }
        public string Number { get; set; }
    }
    
}

