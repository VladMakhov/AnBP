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

        private Entity Entity { get; set; }
  
        private UserConnection UserConnection { get; set; }

        private bool isOlpFirstStage = GetOlpFirstStage();

        private object _case { get; set; }
        private string caseCategory { get; set; }
        private string holding { get; set; }
        private string account { get; set; }
        private object contact { get; set; }
        private object activity { get; set; }
        private Guid mainServiceGroup { get; set; }
        private Guid extraServiceGroup { get; set; }
        private object eis { get; set; }
        private object account { get; set; }
        private Guid accountId { get; set; }
        private Guid holding { get; set; }
        

        public override void OnInserting(object sender, EntityAfterEventArgs e)
        {
            base.OnInserting(sender, e);
            _case = (Entity)sender;
            ProcessEmailRequest();
        }

        private void ProcessEmailRequest()
        {
            // Чтение карточки контакта из обращения
            contact = ReadContactFromCase();
            
            // Нет (СПАМ) - 2 ЭТАП
            if (contact.type == "Клиент не определён/Спам")
            {
                // Спам на обращении + 1 линия поддержки
                UpdateCaseToFirstLineSupport();
                return;
            }

            // email родительской активности
            activity = GetParentActivityFromCase();

            /**
            * Чтение всех основных групп для выделения подходящей основной группы
            * Найти основную группу по email в кому/копия
            * TODO Enum: ServiceGroupType
            */
            mainServiceGroup = SortServiceGroupByEmailAndCopies(GetServiceGroup(ServiceGroupType.Main));
            
            /**
            * Чтение дежурной группы
            * Найти дежурную группу по email в кому/копия
            * TODO Enum: ServiceGroupType
            */
            extraServiceGroup = SortServiceGroupByEmailAndCopies(GetServiceGroup(ServiceGroupType.Extra));
            
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
            if (contact.company != Guid.Empty && !isOlpFirstStage && (contact.type == "Сотрудник" || contact.type == "Поставщик"))
            {
                caseCategory = "Сотрудник/Поставщик";
                /**
                *  Категория (Сотрудник/Поставщик) -2 этап
                * Выставить 1 линию поддержки
                */
                SetFirstLineSupport();
                return;
            }

            // 1 Этап (Сотрудник/Поставщик)
            if (isOlpFirstStage && (contact.type == "Сотрудник" || contact.type == "Поставщик"))
            {
                caseCategory = "Сотрудник/Поставщик";
                FirstStage();
            }

            // Нет (Новый)
            if (contact.company == Guid.Empty || contact.type == Guid.Empty)
            {
                account = GetAccountFromAccountCommutication(GetAccountCommunication());

                // Да (Поставщик)
                if (account != Guid.Empty && account.Type == "Поставщик")
                {
                    caseCategory = "Поставщик";
                    SetContactType();
                    if (isOlpFirstStage)
                    {
                        FirstStage();
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
                    SetAeroclubOnContact();
                    if (isOlpFirstStage)
                    {
                        FirstStage();
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
            if (contact.company != Guid.Empty && (contact.type == "Клиент" || contact.type == "Клиент не определен/Спам"))
            {
                EisPath();
            }
        }

        private void EisPath()
        {
            eis = SendEisRequest();
            // Да
            if (eis.code == 200 || (eis.code == 200 && contact.aeroclubCheck))
            {
                account = GetAccountFromEisResponse();
                
                accountId = account.id;

                RefreshEmails();

                RefreshPhones();

                RefreshContact();

                SetHolding();

                // Чтение карточки контакта после обновления
                ReadContactAfterRefreshing();
            }

            // Нет по домену и нет по ЕИС и (пустой тип или СПАМ)
            if (eis.code == 200 && contact.account == Guid.Empty && (contact.type == Guid.Empty || contact.type == "Клиент не определен/Спам"))
            {
                // категория СПАМ
                caseCategory = "Клиент не определен/Спам";
                if (isOlpFirstStage)
                {
                    SetSpamOnCase();
                    FirstStage();
                }
                else
                {
                    SetSpamOnCase();   
                    return;
                }
            }

            // Нет по ЕИС
            // Да
            if (contact.company != Guid.Empty)
            {
                // Актуализировать тип контакта + Email 
                RefreshContactCompanyAndEmail();
            }
            // Нет
            else
            {
                // Актуализировать компанию контакта + Email
                RefreshContactTypeAndEmail();
            }

             // Найти данные компании привязанной к контакту ненайденного в ЕИС
            contact = ReadContactBindedToEis();
            
            // Выставить холдинг компании контакта
            holding = contact.holding;

            // Чтение карточки контакта после обновления
            ReadContactAfterRefreshing();
        }

        /** 
        * Чтение карточки контакта после обновления
        */
        private void ReadContactAfterRefreshing()
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
        private void goto4(bool extraSG, Guid selectedSGId, object case)
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
        private void goto5(bool extraSG, Guid selectedSGId, object activity)
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
        private void goto6(Guid selectedSG, object case, object activity, bool isOlpFirstStage, Guid selectedSG)
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
            //TODO
        }

        private object ReadContactFromCase()
        {
            // TODO            
        }

        private void UpdateCaseToFirstLineSupport()
        {
            // TODO
        }

        private object GetParentActivityFromCase()
        {
            // TODO
        }

        private List<Guid> GetServiceGroup(ServiceGroupType type)
        {
            // TODO
        }

        private Guid SortServiceGroupByEmailAndCopies()
        {
            // TODO
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

        private List<object> GetAccountCommunication()
        {
            // TODO
        }

        private void GetAccountFromAccountCommutication()
        {
            // TODO
        }

        private void SetAeroclubOnContact()
        {
            // TODO    
        }

        private void SetSpamOnCase()
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

        private void FirstStage()
        {
            // TODO
        }

        private void SendBookAutoreply()
        {
            // TODO
        }

        private void SetAutonotification()
        {
            // TODO
        }

        private void SetContactType()
        {
            // TODO
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
            // TODO
        }

        private void RefreshPhones()
        {
            // TODO
        }

        private void RefreshContact()
        {
            // TODO
        }

        private void RefreshContactCompanyAndEmail()
        {
            // TODO
        }

        private void ReadContactBindedToEis()
        {
            // TODO
        }

        private void method()
        {
            // TODO
        }

        private void method()
        {
            // TODO
        }

        private void method()
        {
            // TODO
        }

        private void method()
        {
            // TODO
        }

        private void method()
        {
            // TODO
        }

        private void method()
        {
            // TODO
        }

    }
}

