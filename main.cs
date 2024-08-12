using BPMSoft.Configuration;
using BPMSoft.Core;
using BPMSoft.Core.DB;
using BPMSoft.Core.Entities;
using BPMSoft.Core.Entities.Events;
using Common.Logging;
using System;
using System.Collections.Generic;


namespace AnseremPackage
{
    [EntityEventListener(SchemaName = nameof(Case))]
    public class ProcessingEmailRequestListener : BaseEntityEventListener
    { 

        private Entity Entity { get; set; }
  
        private UserConnection UserConnection { get; set; }

        public override void OnInserting(object sender, EntityAfterEventArgs e)
        {
            base.OnInserting(sender, e);
            case = (Entity)sender; 
            ProcessEmailRequest(case.GetColumnValue("Id"));
        }

        public void ProcessEmailRequest(string caseId)
        {
            isOlpFirstStage = SysSettings.Get("OLP: Этап 1");
            string caseCategory;
            string holding;
            string account;
            
            // Чтение карточки контакта из обращения
            var contact = ReadContactFromCase(case);
            
            // Нет (СПАМ) - 2 ЭТАП
            if (contact.type == "Клиент не определён/Спам")
            {
                // Спам на обращении + 1 линия поддержки
                UpdateCaseToFirstLineSupport(case);
                return;
            }

            // email родительской активности
            var activity = GetParentActivityFromCase(case);

            /**
            * Чтение всех основных групп для выделения подходящей основной группы
            * Найти основную группу по email в кому/копия
            */
            var mainServiceGroup = SortMainServiceGroupByEmailAndCopies(
                GetMainServiceGroup(), activity.Email, activity.Copy);
            
            /**
            * Чтение дежурной группы
            * Найти дежурную группу по email в кому/копия
            */
            var extraServiceGroup = SortExtraServiceGroupByEmailAndCopies(
                GetExtraServiceGroup(), activity.Email, activity.Copy);

            // Получить Email контакта и домен
            var email = GetEmailFromActivity(activity);
            var domain = GetDomainFromActivity(activity);
            var theme = GetThemeFromActivity(activity);
            var siburTheme = GetSiburThemeFromActivity(activity);
            var body = GetBodyFromActivity(activity);
            
            /**
            * Добавить TRAVEL
            * Поставить отменено на всех отмененных тревел обращениях
            */
            if (!string.IsNullOrEmpty(theme))
            {
                SetTravelParameter(theme, body);
            }
            
            /**
            * Добавить Релокация - СИБУР
            * Поставить отменено на всех отмененных тревел обращениях - Копия
            */
            if (!string.IsNullOrEmpty(siburTheme))
            { 
                SetRelocationParameter(siburTheme, body);
            }

            // Да (Сотрудник) - 2 ЭТАП
            if (contact.Company != Guid.Empty && !isOlpFirstStage && (contact.type == "Сотрудник" || contact.type == "Поставщик"))
            {
                caseCategory = "Сотрудник/Поставщик";
                /**
                *  Категория (Сотрудник/Поставщик) -2 этап
                * Выставить 1 линию поддержки
                */
                SetFirstLineSupport(case, caseCategory);
                return;
            }

            // 1 Этап (Сотрудник/Поставщик)
            if (isOlpFirstStage && (contact.type == "Сотрудник" || contact.type == "Поставщик"))
            {
                caseCategory = "Сотрудник/Поставщик";
                FirstStage();
            }

            // Нет (Новый)
            if (contact.Company == Guid.Empty || contact.type == Guid.Empty)
            {
                account = GetAccountFromAccountCommutication(GetAccountCommunication());

                // Да (Поставщик)
                if (account != Guid.Empty && account.Type == "Поставщик")
                {
                    caseCategory = "Поставщик";
                    SetContactType(contact, "Поставщик");
                    if (isOlpFirstStage)
                    {
                        // TODO Fill in parametrs
                        FirstStage();
                    }
                    else
                    {
                        SetFirstLineSupport(case, caseCategory);
                    }
                }

                // Да (Аэроклуб)
                if (account != Guid.Empty && account.Type == "Наша компания")
                {
                    caseCategory = "Наша компания";
                    SetAeroclubOnContact(contact, "Наша компания");
                    if (isOlpFirstStage)
                    {
                        // TODO Fill in parametrs
                        FirstStage();
                    }
                    else
                    {
                        SetFirstLineSupport(case, caseCategory);
                    }
                }

                // Да (Компания/Холдинг) или потенциальный СПАМ
                EisPath(email);
            }

            // Да (Клиент, СПАМ)
            if (contact.Company != Guid.Empty && (contact.type == "Клиент" || contact.type == "Клиент не определен/Спам"))
            {
                EisPath(email);
            }
        }

        public void SetFirstLineSupport(Entity case, string caseCategory)
        {
            // TODO Выставить 1 линию поддержки + Завершение БП
            return;
        }

        public void SetSecondLineSupport(Entity case, string caseCategory)
        {
            return;
        }

        public void SetThirdLineSupport(Entity case, string caseCategory)
        {
            return;
        }

        public void EisPath(string email, string isOlpFirstStage)
        {
            var response = SendEisRequest(email);
            string caseCategory;
            // Да
            if (response.Code == 200 || (response.Code == 200 && contact.AeroclubCheck))
            {
                /** 
                * Найти данные компании, привязанной к контакту
                * Заполнение ид. контакта
                * Обновление добавление почт и телефонов контакта
                * Актуализировать данные контакта + тип клиент
                * Выставить холдинг компании контакта
                */

                // TODO Proper naming
                // Чтение карточки контакта после обновления
                Method();
            }

            // Нет по домену и нет по ЕИС и (пустой тип или СПАМ)
            if (response.Code == 200 && contact.Account == Guid.Empty && (contact.type == Guid.Empty || contact.type == "Клиент не определен/Спам"))
            {
                // категория СПАМ
                caseCategory = "Клиент не определен/Спам";
                if (isOlpFirstStage)
                {
                    SetSpamOnCase(caseCategory, urgency, importancy);
                    // TODO Fill in parametrs
                    FirstStage();
                }
                else
                {
                    SetSpamOnCase(caseCategory, urgency, importancy);   
                    return;
                }
            }

            // Нет по ЕИС
            // Да
            if (contact.Company != Guid.Empty)
            {
                // Актуализировать тип контакта + Email 

                // Найти данные компании привязанной к контакту ненайденного в ЕИС

                // Выставить холдинг компании контакта

                // TODO Proper naming
                // Чтение карточки контакта после обновления
                Method();
            }
            // Нет
            if (contact.Company == Guid.Empty)
            {
                // Актуализировать компанию контакта + Email

                // Найти данные компании привязанной к контакту ненайденного в ЕИС

                // Выставить холдинг компании контакта

                // TODO Proper naming
                // Чтение карточки контакта после обновления
                Method();
            }
        }

        public void FirstStage()
        {

        }

        public void SendBookAutoreply()
        {

        }

        public void SetAutonotification(object activity)
        {

        }

        /** 
        * Чтение карточки контакта после обновления
        */
        public void Method()
        {
            /**
            * Выставить признак ВИП Платформы
            * Выставить Признак ВИП
            * Выставить параметр "Компания" в ЕИС 
            * Добавить контрагента в Email
            * */

            mainServiceGroup = GetMainSG(company, vip);

            // Найдена ли группа по компании ВИП Платформа?
            // Этап 1
            if (!extraSG && !isOlpFirstStage)
            {
                // TODO Параметры придумать надо
                // Найти ГО основную по графику работы 
                GetMainSgBasedOnTimetable();
                goto4();
            }

            // Да
            else if (isOlpFirstStage)
            {
                // Есть ГО
                if (!extraSG)
                {
                    // TODO Параметры
                    GetMainSgBasedOnTimetableOlpFirstStage();
                }

                // Какую ГО установить?
                if (extraSGFromAndCopyId && extraSG && !mainSGFromAndCopyId)
                {
                    goto5();
                }

                else if (!extraSG)
                {
                    // TODO Переделать под кастомные автоответы!
                    SendBookAutoreply();
                    SetAutonotification(activity);
                    goto6();
                }

                else if (mainServiceGroup && extraSG)
                {
                    if (extraServiceGroup && extraSG)
                    {
                        goto5();
                    }
                    else if (selectedSGId && !extraSG)
                    {
                        goto6();
                    }
                    else
                    {
                        // Найти ГО дежурную по графику работы
                        GetExtraSGBaseOnTimetable();

                        if (selectedSGId && (!extraSG || contact.email.contains("NOREPLY@") || contact.email.contains("NO-REPLY@") || contact.email.contains("EDM@npk.team")))
                        {
                            goto6();
                        }
                        else if (!selectedSGId && (!contact.email.contains("NOREPLY@") && !contact.email.contains("NO-REPLY@") && !contact.email.contains("EDM@npk.team")))
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

        // DONE
        public void goto4(bool extraSG, Guid selectedSGId, object case)
        {
            // Дежурная ГО 2 линия поддержки
            if (extraSG && isVipClient)
            {
                selectedSGId = "OLP:ГО Дежурная группа";
                goto5(selectedSGId);
            }

            // Основная клиентская/ВИП Платформа
            if (!extraSG)
            {
                goto5(selectedSGId);
            }

            // Общая 1 линия поддержки
            if (extraSG && isVipClient)
            {
                selectedSGId = "OLP:ГО Общая 1 линия поддержки";
                UpdateCaseToFirstLineSupport(case);
                goto7();
            }
        }
        
        // DONE
        public void goto5(bool extraSG, Guid selectedSGId, object activity)
        {
            // TODO Найти ГО дежурную из кому/копии по графику работы
        
            Guid serviceGroupId = GetExtraSGFromAndCopyBasedOnTimeTable();
            // TODO
            extraSG =  UnimplementedMethod();
        
            // дежурная ГО найдена или есть основная почта
            if (selectedSGId)
            {
                goto6();
            }
            else
            {
                // TODO Найти основную ГО для контакта по компаниям
            
                // TODO
                bool isDutyGroup = UnimplementedMethod();
            
                // TODO
                serviceGroupId  = UnimplementedMethod();
            
                if (selectedSGId == Guid.Empty)
                {
                    selectedSGId = "Ид. экстра группы из кому/копии";
                    SendBookAutoreply();
                    SetAutonotification(activity);
                    goto6();
                }
                else
                {
                    // [#Ид. экстра группы из кому/копии#] ... extraSG or selectedSGId?
                    selectedSGId = extraSG;
                    SendBookAutoreply();
                }
            }
        }
        
        // DONE
        public void goto6(Guid selectedSG, object case, object activity, bool isOlpFirstStage, Guid selectedSG)
        {
            object SG = GetSG(selectedSG);

            if (SG.type == "ВИП Платформа")
            {
                var priority = "Важно";
                SetSecondLineSupport();
                goto7();
            }

            if (SG.isClientVip)
            {
                var priority = "Важно";
                if (SG.vipDistribution)
                {
                    var thirdLineSupport = GetThirdLineSupport();
                    if (thirdLineSupport)
                    {
                        SetThirdLineSupport(case);
                        goto7();
                    }
                }
                SetSecondLineSupport(case);
                goto7();
            }

            var firstLineSupport = GetFirstLineSupport();

            if (activity.priority == "Высокий")
            {
                var priority = "Важно";
            }

            if (firstLineSupport)
            {
                SetSecondLineSupport(case);
                goto7();
            }
            if (!firstLineSupport && isOlpFirstStage)
            {
                selectedSG = "OLP:ГО Общая 1 линия поддержки";
                SetFirstLineSupport();
                goto7();
            }
        }
        
        // DONE
        public void goto7(object EIS)
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

    }
}