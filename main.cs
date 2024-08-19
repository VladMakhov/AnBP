using System;
using System.Collections.Generic;
using Common.Logging;
using BPMSoft.Configuration;
using BPMSoft.Core;
using BPMSoft.Core.DB;
using BPMSoft.Core.Entities;
using BPMSoft.Core.Entities.Events;


namespace AnseremPackage
{

    [EntityEventListener(SchemaName = nameof(Case))]
    private class ProcessingEmailRequestListener : BaseEntityEventListener
    { 
        // CONSTS
        private const Guid SERVICE_GROUP_TYPE_MAIN = new Guid("C82CA04F-5319-4611-A6EE-64038BA89D71");
       
        private const Guid SERVICE_GROUP_TYPE_EXTRA = new Guid("DC1B5435-6AA1-4CCD-B950-1C4ADAB1F8AD");
       
        private const Guid CONTACT_TYPE_SUPPLIER = new Guid("260067DD-145E-4BB1-9C91-6BF3A36D57E0");
       
        private const Guid CONTACT_TYPE_EMPLOYEE = new Guid("60733EFC-F36B-1410-A883-16D83CAB0980");
       
        private const Guid CONTACT_TYPE_CLIENT = new Guid("00783ef6-f36b-1410-a883-16d83cab0980");
        // CONSTS

        private Entity Entity { get; set; }
  
        private UserConnection UserConnection { get; set; }

        private bool isOlpFirstStage = GetOlpFirstStage();

        private string caseCategory { get; set; }
                        
        private object contact { get; set; }
        
        private object activity { get; set; }
        
        private object eis { get; set; }
        
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

        public override void OnInserting(object sender, EntityAfterEventArgs e)
        {
            base.OnInserting(sender, e);
            var _case = (Entity)sender;
            caseId = _case.GetTypedColumnValue<Guid>("Id");

            // Чтение карточки контакта из обращения
            contact = ReadContactFromCase(_case.GetTypedColumnValue<Guid>("ContactId"));
            
            // Нет (СПАМ) - 2 ЭТАП
            if (contact.GetTypedColumnValue<Guid> == "Клиент не определён/Спам")
            {
                // Спам на обращении + 1 линия поддержки
                UpdateCaseToFirstLineSupport();
                return;
            }

            parentActivityId = _case.GetTypedColumnValue<Guid>("ParentActivityId")

            // email родительской активности
            activity = GetParentActivityFromCase();

            /**
            * Чтение всех основных групп для выделения подходящей основной группы
            * Найти основную группу по email в кому/копия
            */
            mainServiceGroup = GetServiceGroup(SERVICE_GROUP_TYPE_MAIN);
            
            /**
            * Чтение дежурной группы
            * Найти дежурную группу по email в кому/копия
            */
            extraServiceGroup = GetServiceGroup(SERVICE_GROUP_TYPE_EXTRA);
            
            /**
            * Добавить TRAVEL
            * Поставить отменено на всех отмененных тревел обращениях // TODO
            */  
            SetTravelParameter(); // TODO
            
            /**
            * Добавить Релокация - СИБУР
            * Поставить отменено на всех отмененных тревел обращениях - Копия // TODO
            */
            SetSiburParameter(); // TODO
            
            // Да (Сотрудник) - 2 ЭТАП
            if (contact.GetTypedColumnValue<Guid>("Account") != Guid.Empty && !isOlpFirstStage && 
                (contact.GetTypedColumnValue<Guid>("Type") == "Сотрудник" || contact.GetTypedColumnValue<Guid>("Type") == "Поставщик"))
            {
                caseCategory = "Сотрудник/Поставщик";
                /**
                * Категория (Сотрудник/Поставщик) -2 этап
                * Выставить 1 линию поддержки
                */
                SetFirstLineSupport();
                return;
            }

            // 1 Этап (Сотрудник/Поставщик)
            if (isOlpFirstStage && (contact.GetTypedColumnValue<Guid>("Type") == "Сотрудник" || contact.GetTypedColumnValue<Guid>("Type") == "Поставщик"))
            {
                caseCategory = "Сотрудник/Поставщик";
                goto2();
            }

            // Нет (Новый)
            if (contact.GetTypedColumnValue<Guid>("Account") == Guid.Empty || contact.GetTypedColumnValue<Guid>("Type") == Guid.Empty)
            {
                account = FetchAccountById(GetAccountIdFromAccountCommunication());

                // Да (Поставщик)
                if (account != null && account.GetTypedColumnValue<Guid>("Type") == "Поставщик")
                {
                    caseCategory = "Сотрудник/Поставщик";
                    SetContactType(contact.GetTypedColumnValue<Guid>("Id"), CONTACT_TYPE_SUPPLIER);
                    if (isOlpFirstStage)
                    {
                        goto2();
                    }
                    else
                    {
                        SetFirstLineSupport();
                    }
                }

                // Да (Аэроклуб)
                if (account != Guid.Empty && account.type == "Наша компания")
                {
                    caseCategory = "Наша компания";
                    SetContactType(contact.GetTypedColumnValue<Guid>("Id"), CONTACT_TYPE_EMPLOYEE);
                    if (isOlpFirstStage)
                    {
                        goto2();
                    }
                    else
                    {
                        SetFirstLineSupport();
                    }
                }

                // Да (Компания/Холдинг) или потенциальный СПАМ
                EisPath();
            }

            // Да (Клиент, СПАМ)
            if (contact.GetTypedColumnValue<Guid>("Account") != Guid.Empty && 
                (contact.GetTypedColumnValue<Guid>("Type") == "Клиент" || contact.GetTypedColumnValue<Guid>("Type") == "Клиент не определен/Спам"))
            {
                EisPath();
            }
        }

        private void EisPath()
        {
            eis = SendEisRequest(); // TODO интеграция + объект ЕИС
            // Да
            if (eis.code == 200 || (eis.code == 200 && contact.aeroclubCheck))
            {
                account = FetchAccountByEis(eis.account);
                
                RefreshEmails(); // TODO

                RefreshPhones(); // TODO

                RefreshContact(account.GetTypedColumnValue<Guid>("Id"), contact.GetTypedColumnValue<Guid>("Id"));

                holding = account.GetTypedColumnValue<Guid>("OlpHolding");

                // Чтение карточки контакта после обновления
                ReadContactAfterRefreshing();
            }

            // Нет по домену и нет по ЕИС и (пустой тип или СПАМ)
            if (eis.code == 200 && contact.account == Guid.Empty && (contact.GetTypedColumnValue<Guid>("Type") == Guid.Empty || contact.GetTypedColumnValue<Guid>("Type") == "Клиент не определен/Спам"))
            {
                // категория СПАМ
                caseCategory = "Клиент не определен/Спам";
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
            
            GetMainServiceGroup(); // TODO

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
                    GetMainServiceGroupBasedOnTimetableOlpFirstStage(); // TODO
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
                    // TODO Переделать под кастомные автоответы!
                    SendBookAutoreply();
                    SetAutonotification();
                    goto6();
                }

                // Указана осн ГО в кому/копии
                else if (mainServiceGroup == Guid.Empty && isExtraServiceGroup)
                {
                    if (extraServiceGroup && extraServiceGroup)
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
                            SendBookAutoreply();
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
                // TODO Параметры придумать надо
                goto4();
            }
        }

        private void goto4()
        {
            // Дежурная ГО 2 линия поддержки
            if (extraServiceGroup && isVipClient)
            {
                selectedServiceGroupId = "OLP:ГО Дежурная группа";
                goto5();
            }

            // Основная клиентская/ВИП Платформа
            if (!extraServiceGroup)
            {
                goto5();
            }

            // Общая 1 линия поддержки
            if (extraServiceGroup && isVipClient)
            {
                selectedServiceGroupId = "OLP:ГО Общая 1 линия поддержки";
                UpdateCaseToFirstLineSupport();
                goto7();
            }
        }
        
        private void goto5()
        {
            // TODO Найти ГО дежурную из кому/копии по графику работы
            GetExtraServiceGroupFromAndCopyBasedOnTimeTable();

            // дежурная ГО найдена или есть основная почта
            if (selectedServiceGroupId)
            {
                goto6();
            }
            else
            {
                // TODO Найти основную ГО для контакта по компаниям
                GetMainServiceGroupForContact();
            
                if (selectedServiceGroupId == Guid.Empty)
                {
                    selectedServiceGroupId = "Ид. экстра группы из кому/копии";
                    SendBookAutoreply();
                    SetAutonotification();
                    goto6();
                }
                else
                {
                    // [#Ид. экстра группы из кому/копии#] ... extraServiceGroup or selectedServiceGroupId????
                    selectedServiceGroupId = extraServiceGroup;
                    SendBookAutoreply();
                }
            }
        }
        
        private void goto6()
        {
            object serviceGroup = GetServiceGroupById();

            if (serviceGroup.type == "ВИП Платформа")
            {
                var priority = "Важно";
                SetSecondLineSupport();
                goto7();
            }

            if (serviceGroup.isClientVip)
            {
                var priority = "Важно";
                if (serviceGroup.vipDistribution)
                {
                    var thirdLineSupport = GetThirdLineSupport();
                    if (thirdLineSupport)
                    {
                        SetThirdLineSupport();
                        goto7();
                    }
                }
                SetSecondLineSupport();
                goto7();
            }

            var firstLineSupport = GetFirstLineSupport();

            if (activity.priority == "Высокий")
            {
                var priority = "Важно";
            }

            if (firstLineSupport)
            {
                SetSecondLineSupport();
                goto7();
            }
            if (!firstLineSupport && isOlpFirstStage)
            {
                selectedServiceGroupId = "OLP:ГО Общая 1 линия поддержки";
                SetFirstLineSupport();
                goto7();
            }
        }
        
        private void goto7(object EIS)
        {
            if (EIS.code == 200)
            {
                return;
            }

            if (EIS.orderNumbCheck != Guid.Empty)
            {
               // TODO Собрать услуги для добавления
               
               // TODO Запустить "OLP: Подпроцесс - Обновление услуг контакта v 3.0.1"
            }

            return;
        }

        private bool GetOlpFirstStage()
        {
            string sql = $"""
                SELECT BooleanValue FROM SysSettingsValue 
                WHERE SysSettingsId = (
                    SELECT id FROM SysSettings WHERE Code LIKE 'OLPIsFirstStepToPROD'
                )
                """;
			
			CustomQuery query = new CustomQuery(UserConnection, sql);
			
			using (var db = UserConnection.EnsureDBConnection())
			{
				using (var reader = query.ExecuteReader(db))
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
            string sql = $"""
                UPDATE \"Case\"
                SET 
                    OlpGroupServices = '{ГО Общая 1 линия поддержки}', // TODO
                    OlpSupportLine = '{ОР 1 линия поддержки}', // TODO
                    OlpImportant = '{Не важно}', // TODO
                    OlpUrgency = '{Не срочно}', // TODO
                    Category = '{caseCategory}'
                WHERE ID = '{CaseId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private Activity GetParentActivityFromCase()
        {
            var activity = new Activity(UserConnection);
            Dictionary<string, object> conditions = new Dictionary<string, object> {
                    { nameof(Activity.Id), contactId },
                    { nameof(Activity.TypeId) "Email"} // TODO
                };

            if (activity.FetchFromDB(conditions))
            {
                return activity;
            }
        }

        private Guid GetServiceGroup(Guid type)
        {
            string sql = $"""
                SELECT * FROM OlpServiceGroup
                WHERE Id IS NOT NULL AND
                OlpSgEmail IS NOT NULL AND
                OlpTypeGroupService = '{type}' // TODO
                """;
			
			CustomQuery query = new CustomQuery(UserConnection, sql);
			
			using (var db = UserConnection.EnsureDBConnection())
			{
				using (var reader = query.ExecuteReader(db))
				{
					while (reader.Read())
                    {
                        // TODO Тут должна быть ебанная хуйня из "Найти основную группу по email в кому/копия"
					}
				}
			}
        }

        private void SetTravelParameter()
        {
            if (!string.IsNullOrEmpty(activity.theme))
            {
                // TODO
            }
        }

        private void SetSiburParameter()
        {
            if (!string.IsNullOrEmpty(siburTheme))
            {
               // TODO  
            }
        }

        private Guid GetAccountIdFromAccountCommunication()
        {
            string sql = $"""
                SELECT TOP 1 * FROM AccountCommunication
                WHERE 
                (
                    CommunicationType = '{Почтовый домен}' // TODO 
                    AND 
                    Number = '{Домен Email}' // TODO
                )
                OR
                (
                    CommunicationType = '{Email}' // TODO 
                    AND 
                    Number = '{Email}' // TODO
                )
                """;
			
			CustomQuery query = new CustomQuery(UserConnection, sql);
			
			using (var db = UserConnection.EnsureDBConnection())
			{
				using (var reader = query.ExecuteReader(db))
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
            string sql = $"""
                UPDATE Contact
                SET 
                    Type = '{type}',
                    Account = '{companyId}'
                WHERE id = '{contactId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private void SetSpamOnCase()
        {
            var contactId = contact.GetTypedColumnValue<Guid>("Id");
            string sql = $"""
                UPDATE Contact
                SET 
                    Type = '{Клиент не определен/Спам}', // TODO
                    Account = '{companyId}' // TODO
                WHERE id = '{contactId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private void RefreshContact(Eis eis, Guid accountId, Guid contactId)
        {
            string sql = $"""
                UPDATE Contact
                SET
                    Email = '{Email}', // TODO
                    Account = '{accountId}',
                    OlpBooleanAeroclubCheck = 1,
                    OlpSignVip = '{eis.isVip}',
                    OlpContactProfileConsLink = '{eis.profileLink}',
                    OlpLnFnPat = '{}', // TODO
                    GivenName = '{eis.rusFirstName}',
                    MiddleName = '{eis.rusMiddleName}',
                    Surname = '{eis.rusSurname}',
                    OlpSignVipPlatf = '{eis.isVipPlatform}',
                    OlpIsAuthorizedPerson = '{eis.isAuthorizedPerson}',
                    OlpIsContactPerson = '{eis.isContactPerson}',
                    Type = '{CONTACT_TYPE_CLIENT}',
                    OlpExternalContId = '{eis.idOut}'
                WHERE id = '{contactId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private void RefreshContactCompanyAndEmail()
        {
            var contactId contact.GetTypedColumnValue<Guid>("Id");
            var accountId = account.GetTypedColumnValue<Guid>("Id");
            string sql = $"""
                UPDATE Contact
                SET
                    Email = '{Email}', // TODO
                    Account = '{accountId}',
                    Type = '{CONTACT_TYPE_CLIENT}',
                WHERE id = '{contactId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private void RefreshContactTypeAndEmail()
        {
            var contactId contact.GetTypedColumnValue<Guid>("Id");
            string sql = $"""
                UPDATE Contact
                SET
                    Email = '{Email}', // TODO
                    Type = '{CONTACT_TYPE_CLIENT}',
                WHERE id = '{contactId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private void GetHoldingFromAccountBindedToEis()
        {
            var contactId contact.GetTypedColumnValue<Guid>("Id");
            string sql = $"""
                SELECT OlpHoldingId FROM Contact c 
                INNER JOIN Account a ON a.Id = c.AccountId
                WHERE c.Id = '{contactid}'
                """;
			
			CustomQuery query = new CustomQuery(UserConnection, sql);
			
			using (var db = UserConnection.EnsureDBConnection())
			{
				using (var reader = query.ExecuteReader(db))
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
            string sql = $"""
                UPDATE Activity
                SET Account = '{clientCompanyId}', // TODO
                WHERE id = '{parentActivityId}'
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private Guid GetServiceGroupById(Guid id)
        {
            // TODO
        }

        private void GetMainServiceGroup()
        {
            // TODO
        }

        private void GetMainServiceGroupBasedOnTimetable()
        {
            // TODO
        }

        private void GetMainServiceGroupBasedOnTimetableOlpFirstStage()
        {
            // TODO
        }

        private void GetExtraServiceGroupBaseOnTimetable()
        {
            // TODO
        }

        private void GetExtraServiceGroupFromAndCopyBasedOnTimeTable()
        {
            // TODO
        }

        private void ReadAccount()
        {
            // TODO
        }

        private void SetupHolding()
        {
            // TODO
        }

        private void SetFirstLineSupport()
        {
            // TODO Выставить 1 линию поддержки + Завершение БП
            return;
        }

        private void SetSecondLineSupport()
        {
            // TODO
            return;
        }

        private void SetThirdLineSupport()
        {
            // TODO
            return;
        }

        private void SendBookAutoreply()
        {
            // TODO
        }

        private void SetAutonotification()
        {
            string sql = $"""
                UPDATE Activity
                SET IsAutoSubmitted = '{true}',
                WHERE id = '{parentActivityId}' // TODO Is Parent activity Id needed?
                """;
			CustomQuery query = new CustomQuery(UserConnection, sql);
			query.Execute();
        }

        private void SendEisRequest()
        {
            // TODO
        }

        private object GetAccountFromEisResponse()
        {
            // TODO
        }

        private void RefreshEmails()
        {
            // TODO Сюда ебануть код из "Обновление добавление почт и телефонов контакта"
        }

        private void RefreshPhones()
        {
            // TODO Сюда то же из "Обновление добавление почт и телефонов контакта"
        }

    }
}
