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
        // CONSTS

        // PARAMS 
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

        private Guid importancy { get; set; }

        private Guid urgency { get; set; }
        
        private Guid email { get; set; }
        
        private Guid copies { get; set; }
        // PARAMS 

        // LEGACY
        private Guid MainGroupIdByEmail { get; set; }
        
        private string MainGroupEmailBox { get; set; }
        
        private Guid MainGroupEmailBoxId { get; set; }
        
        private Guid MainEmailBoxIdForReg { get; set; }
        
        private string MainSheduleTypeByMail { get; set; }
        
        private string ExtraGroupEmailBox { get; set; }
        
        private Guid ExtraGroupIdByEmail { get; set; }
        
        private Guid ExtraGroupEmailBoxId { get; set; }
        
        private string ExtraSheduleTypeByMail { get; set; }
        
        private Guid { get; set; }
        
        private Guid { get; set; }
        
        private Guid { get; set; }
        
        private Guid { get; set; }
        // LEGACY

        public override void OnInserting(object sender, EntityAfterEventArgs e)
        {
            base.OnInserting(sender, e);
            var _case = (Entity)sender;
            caseId = _case.GetTypedColumnValue<Guid>("Id");

            // Чтение карточки контакта из обращения
            contact = ReadContactFromCase(_case.GetTypedColumnValue<Guid>("ContactId"));

            // Нет (СПАМ) - 2 ЭТАП
            if (contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_UNDEFINED_CLIENT_SPAM) 
            {
                // Спам на обращении + 1 линия поддержки
                UpdateCaseToFirstLineSupport();
                return;
            }

            parentActivityId = _case.GetTypedColumnValue<Guid>("ParentActivityId")

            // email родительской активности
            activity = GetParentActivityFromCase();

            email = activity.GetTypedColumnValue<string>("Recepient"); // TODO Check for correct grammar

            copies = activity.GetTypedColumnValue<string>("CopyRecepient"); // TODO Check for correct grammar

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
            if (eis.code == 200 && contact.account == Guid.Empty && (contact.GetTypedColumnValue<Guid>("Type") == Guid.Empty || contact.GetTypedColumnValue<Guid>("Type") == CONTACT_TYPE_UNDEFINED_CLIENT_SPAM))
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
                    GetMainServiceGroupBasedOnTimetable(); // TODO Старый код. Вероятно, что другой метод
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

                selectedServiceGroupId = selectedServiceGroupId == Guid.Empty ? etraServiceGroupFromAndCopy : selectedServiceGroupId;

                SendBookAutoreply();
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

                if (serviceGroup.GetParentActivityFromCase<bool>("OlpDistribution"))
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

            var firstLineSupport = GetFirstLineSupport();

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
            if (EIS.code == 200)
            {
                return;
            }

            if (EIS.orderNumbCheck != Guid.Empty)
            {
                // TODO Собрать услуги для добавления
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
            }

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
                    { nameof(Activity.TypeId) "Email"} // TODO
            };

            if (activity.FetchFromDB(conditions))
            {
                return activity;
            }
        }

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
        }

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
                    Type = '{CONTACT_TYPE_UNDEFINED_CLIENT_SPAM}', // TODO
                    Account = '{companyId}' // TODO
                WHERE
                    Id = '{contactId}'
            ";
            CustomQuery query = new CustomQuery(UserConnection, sql);
            query.Execute();
        }

        private void RefreshContact(Eis eis, Guid accountId, Guid contactId)
        {
            string sql = @$"
                UPDATE
                    Contact
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
                SET
                    Email = '{Email}', // TODO
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
                    Email = '{Email}', // TODO
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
                    OlpServiceGroupForOrder = '{}', // TODO
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
                    OlpServiceGroupForOrder = '{}', // TODO
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
                    OlpServiceGroupForOrder = '{}', // TODO
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

        private void refreshPhones()
        {
            // TODO Сюда то же из "Обновление добавление почт и телефонов контакта"
        }

        private void CollectServicesForInsertion()
        {
            // TODO Элемент старого кода   
        }
    }
}
