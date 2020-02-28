#!/apps/dslab/anaconda/python3/bin/python
from code_to_parse_kafka_messages import *

from util import *

deal_columns = {'activeWorkflow': 'ACTIVE_WORKFLOW','approvalRequiredByDate': 'APPROVAL_REQUIRED_BY_DATE',
'businessScenarioId': 'BUSINESS_SCENARIO','cdpFlag': 'CDP_FLAG',
'createdBy': 'CREATED_BY','createdOn': 'CREATED_ON',
'paeUserId': 'CREATOR_PAE_USER_ID','cusotmerUseCase': 'CUSTOMER_USE_CASE',
'customerUseSolution': 'CUSTOMER_USE_SOLUTION','ddaDealDuration': 'DDA_DEAL_DURATION',
'ddaCreatorRole': 'DEAL_CREATOR_ROLE',
'role': 'DEAL_CREATOR_ROLE_NAME',
'dealDuration': 'DEAL_DURATION',
'dealExpBookDate': 'DEAL_EXPECTED_BOOK_DATE',
'dealOrQuote': 'DEAL_OR_QUOTE',
'dealScopeId': 'DEAL_SCOPE',
'dealScope': 'DEAL_SCOPE_NAME',
'dealSource': 'DEAL_SOURCE',
'dealSourceDesc': 'DEAL_SOURCE_NAME',
'dealType': 'DEAL_TYPE ',
'dealTypeDesc': 'DEAL_TYPE_NAME',
'dmUpdateDate': 'DM_UPDATE_DATE',
'expectedCloseDateAsDate': 'EXPECTED_CLOSE_DATE',
'finalBand': 'FINAL_BAND',
'financialSummary': 'FINANCIAL_SUMMARY',
'lastApprovalDate': 'LAST_APPROVAL_DATE',
'mdmDealExpirationDate': 'MDM_DEAL_EXPIRATION_DATE',
'mdmDealStatus': 'MDM_DEAL_STATUS',
'dealStatusDesc': 'MDM_DEAL_STATUS_NAME',
'mdmOptyDescStrategicDesc': 'MDM_OPTY_DESC_STRATEGIC_DESC',
'mdmOptyDescStrategicValue': 'MDM_OPTY_DESC_STRATEGIC_VALUE',
'mdmProdValue': 'MDM_PROD_VALUE ',
'mdmServValue': 'MDM_SERV_VALUE',
'mergedSfdcOptyId': 'MERGED_SFDC_OPTY_ID',
'dealObjectId': 'OBJECT_ID',
'dealId': 'OPTY_ID ',
'dealName': 'OPTY_NAME',
'optyStatus': 'OPTY_STATUS',
'parentObjectId': 'PARENT_OBJECT_ID',
'pdrOptyDescStrategicValue': 'PDR_OPTY_DESC_STRATEGIC_VALUE',
'pdrProdValue': 'PDR_PROD_VALUE',
'pdrServValue': 'PDR_SERV_VALUE',
'prodSalesPath': 'PROD_SALES_PATH ',
'publishDiscountFlag': 'PUBLISH_DISCOUNT_FLAG',
'qfFormId': 'QF_FORM_ID',
'qfFormStatus': 'QF_FORM_STATUS',
'quoteFlag': 'QUOTE_FLAG',
'reopenReason': 'REOPEN_REASON',
'revenueRecognitionFlag': 'REVENUE_RECOG_FLAG',
'rfpFlag': 'RFP_FLAG',
'servSalesPath': 'SERV_SALES_PATH ',
'sfdcActiveFlag': 'SFDC_ACTIVE_FLAG ',
'sfdcIntFlg': 'SFDC_INTEGRATION_FLAG',
'sfdcOptyId': 'SFDC_OPTY_ID',
'sourceOptyNumber': 'SOURCE_OPTY_NUMBER',
'optyNumber': 'OPTY_NUMBER',
'updatedBy': 'UPDATED_BY ',
'updatedOn': 'UPDATED_ON ',
'verticalMarket': 'VERTICAL_MARKET',
'submittedOn': 'SUBMIT_DATE'
}

team_details_columns = {'parentId': 'PARENT_ID', 'cecId': 'MEMBER_CEC_ID', 'ccoId': 'MEMBER_CCO_ID',
                        'firstName': 'MEMBER_FIRST_NAME', 'lastName': 'MEMBER_LAST_NAME', 'role': 'MEMBER_ROLE',
                        'accessLevel': 'MEMBER_ACCESS_LEVEL', 'accessGrantorId': 'ACCESS_GRANTOR_ID',
                        'emailCc': 'EMAIL_CC', 'emailSentFlag': 'EMAIL_SENT_FLAG', 'memberId': 'MEMBER_ID',
                        'memberAccessScope': 'MEMBER_ACCESS_SCOPE', 'memberVisibility': 'MEMBER_VISIBILITY',
                        'objectId': 'OBJECT_ID', 'memberPosition': 'MEMBER_POSITION', 'memberType': 'MEMBER_TYPE',
                        'quoteObjectId': 'QUOTE_OBJECT_ID', 'active': 'ACTIVE'}
deal_contact_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID', 'cecId': 'CONTACT_ID',
                        'contactName': 'CONTACT_NAME', 'contactTypeId': 'CONTACT_TYPE',
                        'contactPosition': 'CONTACT_POSTN_ID', 'contactRow': 'CONTACT_ROW'}
end_customer_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID', 'crId': 'CR_ID',
                        'endCustomerName': 'ACCOUNT_NAME', 'addressLine1': 'ADDR', 'addressLine2': 'ADDR_LINE_2',
                        'addressLine3': 'ADDR_LINE_3', 'city': 'CITY', 'state': 'PROVINCE', 'zipCode': 'ZIPCODE',
                        'country': 'COUNTRY', 'county': 'COUNTY', 'endCustomerType': 'END_CUSTOMER_TYPE',
                        'siebelCompanyId': 'SIEBEL_COMPANY_ID', 'structuredAccount': 'STRUCTURED_ACCOUNT',
                        'accountType': 'ACCOUNT_TYPE'}
deal_node_columns = {'parentId': 'PARENT_ID', 'shareNodeId': 'SHARE_NODE_ID', 'createdBy': 'CREATED_BY',
                     'createdOn': 'CREATED_ON', 'updatedBy': 'UPDATED_BY', 'updatedOn': 'UPDATED_ON',
                     'theater': 'THEATER', 'area': 'AREA', 'region': 'REGION', 'theaterNodeId': 'THEATER_NODE_ID',
                     'areaNodeId': 'AREA_NODE_ID', 'regionNodeId': 'REGION_NODE_ID', 'optyNumber': 'OPTY_NUMBER',
                     'level6': 'LEVEL6_NODE', 'marketSegment': 'MARKET_SEGMENT', 'dealType': 'DEAL_TYPE'}
party_details_columns = {'parentId': 'PARENT_ID', 'partyName': 'PARTY_NAME', 'partyTypeDesc': 'PARTY_TYPE_NAME',
                         'siteId': 'SITE_ID', 'partyId': 'PARTY_ID', 'objectId': 'OBJECT_ID',
                         'partyTypeLevel': 'PARTY_TYPE_LEVEL', 'primaryFlag': 'PRIMARY_FLAG'}
ns_deal_extn_columns = {'parentId': 'PARENT_ID', 'approvalRoute': 'APPROVAL_ROUTE',
                        'approvalRouteDesc': 'APPROVAL_ROUTE_NAME', 'productComp': 'PRODUCT_COMP',
                        'servComp': 'SERVICE_COMPONENT','tssCoreComp': 'TSS_CORE_COMP',
                        'asTransactionComp': 'AS_TRANSACTION_COMP','asSubscriptionComp': 'AS_SUBSCRIPTION_COMP',
                        'asFixedComp': 'AS_FIXED_COMP','rosComp': 'ROS_COMP',
                        'getWellPlanDescr': 'GET_WELL_PLAN_DESCR', 'isSaDeal': 'IS_SA_DEAL',
                        'dsaRegstrDate': 'DSA_REGSTR_DATE', 'ddEngageDate': 'DD_ENGAGE_DATE',
                        'nsSummaryDescr': 'NS_SUMMARY_DESCR', 'exeSummaryInternalDescr': 'EXE_SUMMARY_INTERNAL_DESCR'}
competitor_details_columns = {'parentId': 'PARENT_ID', 'competitorName': 'COMPETITOR_NAME', 'compTech': 'COMP_TECH',
                              'competitorProductName': 'COMP_PROD_NAME', 'competitorType': 'COMP_TYPE',
                              'competitorAdditionalInfo': 'COMP_ADD_INFO', 'objectId': 'OBJECT_ID',
                              'initiatedSource': 'INITIATED_SOURCE', 'source': 'SOURCE'}
non_std_term_columns = {'parentId': 'PARENT_ID', 'nsTermId': 'NSTERM_ID', 'createdBy': 'CREATED_BY',
                        'createdOn': 'CREATED_ON', 'updatedBy': 'UPDATED_BY', 'updatedOn': 'UPDATED_ON',
                        'tssCore': 'TSS_CORE', 'asSubscription': 'AS_SUBSCRIPTION', 'asTransaction': 'AS_TRANSACTION',
                        'ros': 'ROS', 'asFixed': 'AS_FIXED', 'nsTermName': 'NSTERM_NAME', 'objectId': 'OBJECT_ID'}
state_transitions_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID', 'state': 'STATE',
                             'transitionDate': 'TRANSITION_DATE', 'transitionInitiatedBy': 'TRANSITION_INITIATED_BY',
                             'source': 'SOURCE', 'createdBy': 'CREATED_BY', 'createdOn': 'CREATED_ON',
                             'dealStatusDesc': 'MDM_DEAL_STATUS', 'quoteStatusDesc': 'PDR_DEAL_STATUS'}
qf_response_columns = {'parentId': 'PARENT_ID', 'questionId': 'QUESTION_ID', 'value': 'VALUE_NAME'}
quotes_columns = {'parentId': 'PARENT_ID', 'quoteId': 'QUOTE_ID', 'createdBy': 'CREATED_BY',
                  'createdOn': 'CREATED_ON', 'updatedBy': 'UPDATED_BY', 'updatedOn': 'UPDATED_ON', 'active': 'ACTIVE',
                  'netPriceDeal': 'NET_PRICE_DEAL', 'frameAgreement': 'FRAME_AGREEEMENT', 'sourceType': 'SOURCE_TYPE',
                  'prodPricingComment': 'PROD_PRICING_COMMENT', 'servPricingComment': 'SERV_PRICING_COMMENT',
                  'bomState': 'BOM_STATE', 'lastPricedDate': 'LAST_PRICED_DATE', 'cartId': 'CART_ID',
                  'siteObjectId': 'SITE_OBJECT_ID', 'bomSource': 'BOM_SOURCE', 'contractualDisc': 'CONTRACTUAL_DISC',
                  'quoteImportDate': 'QUOTE_IMPORT_DATE', 'quoteName': 'QUOTE_NAME', 'tradeInState': 'TRADEIN_STATE',
                  'cbnStatusId': 'CBN_STATUS', 'cbnDistiId': 'CBN_DISTI_ID', 'isQ2oEligible': 'IS_Q2O_ELIGIBLE',
                  'autoNegotiateNs': 'AUTO_NEGOTIATE_NS', 'cbnSplitEligible': 'CBN_SPLIT_ELIGIBLE',
                  'isDistiQ2oEligible': 'IS_DISTI_Q2O_ELIGIBLE', 'tab': 'TAB_FLAG', 'tabSubtrack': 'TAB_SUBTRACK',
                  'tabPeriod': 'TAB_PERIOD', 'billingTerm': 'BILLING_TERM', 'fulfillmentType': 'FULFILLMENT_TYPE',
                  'fulfillmentSourceId': 'FULFILLMENT_SOURCE_ID', 'copyFromQuote': 'COPY_FROM_QUOTE',
                  'quoteStatusId': 'QUOTE_STATUS', 'intendedUse': 'INTENDED_USE', 'quoteCategory': 'QUOTE_CATEGORY',
                  'quoteExpirationDate': 'QUOTE_EXPIRATION_DATE', 'stringent': 'IS_STRINGENT',
                  'finalized': 'IS_FINALIZED', 'customerCatalogFlag': 'CUSTOMER_CATALOG_FLAG', 'isFaDeal': 'IS_FA_FLAG',
                  'federalFlg': 'FEDERAL_FLG', 'taa': 'TAA_FLG',
                  'quoteStatusDesc': 'QUOTE_STATUS_NAME', 'gdrFlag': 'GDR_FLAG', 'crpFlag': 'CRP_FLAG',
                  'leadPartners': 'LEAD_PARTNERS', 'leadPartnerType': 'LEAD_PARTNER_TYPE', 'faDealId': 'FA_DEAL_ID',
                  'erpPriceListId': 'ERP_PRICE_LIST_ID', 'priceListId': 'PRICE_LIST_ID', 'currencyCode': 'CURRENCY_CODE',
                  'stdIncrFlag': 'STD_INCR_FLAG'}
technology_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID', 'dealObjectId': 'DEAL_OBJECT_ID',
                      'technologyName': 'TECHNOLOGY', 'percentageOfMix': 'PERCENTAGE_OF_MIX'}
deal_reopen_reason_columns = {'parentId': 'PARENT_ID', 'reopenReasonDesc': 'REOPEN_REASON', 'createdBy': 'CREATED_BY',
                              'createdOn': 'CREATED_ON', 'updatedBy': 'UPDATED_BY', 'updatedOn': 'UPDATED_ON'}
disti_fillment_details_columns = {'parentId': 'PARENT_ID', 'fulfillmentObjID': 'OBJECT_ID', 'startDate': 'START_DATE',
                                  'sourceProfileId': 'SOURCE_PROFILE_ID', 'deviationId': 'DEVIATION_ID',
                                  'devProcessRemark': 'DEV_PROCESS_REMARK', 'revisionNumber': 'REVISION_NUMBER',
                                  'endDate': 'END_DATE', 'nextAction': 'NEXT_ACTION',
                                  'lastUpdateDate': 'LAST_UPDATE_DATE', 'authorizationNumber': 'AUTHORIZATION_NUMBER',
                                  'devProcessDescription': 'DEV_PROCESS_DESCRIPTION',
                                  'linkedDeviationId': 'LINKED_DEVIATION_ID', 'deviationStatus': 'DEVIATION_STATUS',
                                  'devRetryCount': 'DEV_RETRY_COUNT', 'devProcessStatus': 'DEV_PROCESS_STATUS',
                                  'queueObjectId': 'QUEUE_OBJECT_ID'}
behaviour_rewards_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID', 'quoteObjectId': 'QUOTE_OBJECT_ID',
                             'dealObjectId': 'DEAL_OBJECT_ID', 'bhRewardId': 'BH_REWARD_ID',
                             'bhRewardCode': 'BH_REWARD_CODE', 'bhRewardName': 'BH_REWARD_NAME',
                             'bhRewardVersion': 'BH_REWARD_VERSION', 'isBhRewardApproved': 'IS_BH_REWARD_APPROVED',
                             'isBhRewardSelectable': 'IS_BH_REWARD_SELECTABLE', 'bundleType': 'BUNDLE_TYPE',
                             'isBhRewardSelected': 'IS_BH_REWARD_SELECTED', 'programName': 'PROGRAM_NAME',
                             'bhRewardVersionApplied': 'BH_REWARD_VERSION_APPLIED', 'bundleTypeDesc': 'BUNDLE_TYPE_DESC'}
standard_approval_route_info_columns = {'parentId': 'PARENT_ID', 'approvalScope': 'APPROVAL_SCOPE',
                                        'precedence': 'PRECEDENCE', 'apprQueueSeq': 'APPR_QUEUE_SEQ',
                                        'apprCecId': 'APPR_CEC_ID', 'apprRole': 'APPR_ROLE',
                                        'apprTeamId': 'APPR_TEAM_ID', 'apprTeamName': 'APPR_TEAM_NAME',
                                        'defaultTeamMember': 'DEFAULT_TEAM_MEMBER', 'decision': 'DECISION',
                                        'bhRewardCode': 'BH_REWARD_CODE', 'canModifyDiscount': 'CAN_MODIFY_DISCOUNT',
                                        'isDynamicallyAdded': 'IS_DYNAMICALLY_ADDED',
                                        'canAddApprover': 'CAN_ADD_APPROVER', 'readOnlyFlag': 'READ_ONLY_FLAG',
                                        'decisionSla': 'DECISION_SLA', 'reminder1Sla': 'REMINDER1_SLA',
                                        'reminder2Sla': 'REMINDER2_SLA', 'escSla': 'ESC_SLA', 'escType': 'ESC_TYPE',
                                        'escActionType': 'ESC_ACTION_TYPE', 'escLevel': 'ESC_LEVEL',
                                        'escAppr1': 'ESC_APPR_1', 'escAppr2': 'ESC_APPR_2', 'escAppr3': 'ESC_APPR_3',
                                        'escAppr1TeamName': 'ESC_APPR_1_TEAM_NAME', 'createdBy': 'CREATED_BY',
                                        'escAppr2TeamName': 'ESC_APPR_2_TEAM_NAME', 'updatedOn': 'UPDATED_ON',
                                        'escAppr3TeamName': 'ESC_APPR_3_TEAM_NAME', 'updatedBy': 'UPDATED_BY',
                                        'isCurrentApprover': 'IS_CURRENT_APPROVER', 'decisionSource': 'DECISION_SOURCE',
                                        'decisionDate': 'DECISION_DATE', 'nextSlaNotifn': 'NEXT_SLA_NOTIFN',
                                        'decisionPendingFromTs': 'DECISION_PENDING_FROM_TS', 'createdOn': 'CREATED_ON',
                                        'escAppr1TeamMailerAlias': 'ESC_APPR_1_TEAM_MAILER_ALIAS',
                                        'escAppr2TeamMailerAlias': 'ESC_APPR_2_TEAM_MAILER_ALIAS',
                                        'escAppr3TeamMailerAlias': 'ESC_APPR_3_TEAM_MAILER_ALIAS',
                                        'isImplicitAppr': 'IS_IMPLICIT_APPR', 'objectId': 'OBJECT_ID'}
billing_address_columns = {'parentId': 'PARENT_ID', 'billingId': 'BILLTO_LOCATION', 'partyId': 'PARTY_ID',
                           'customerName': 'CUSTOMER_NAME', 'customerActSiteID': 'CUST_ACCT_SITE_ID',
                           'custAccountId': 'CUST_ACCOUNT_ID', 'accountNumber': 'ACCOUNT_NUMBER',
                           'address1': 'ADDRESS1', 'address2': 'ADDRESS2', 'address3': 'ADDRESS3',
                           'address4': 'ADDRESS4', 'country': 'COUNTRY', 'city': 'CITY', 'postalCode': 'POSTAL_CODE',
                           'state': 'STATE', 'province': 'PROVINCE', 'county': 'COUNTY', 'countryDesc': 'COUNTRY_DESC',
                           'objectId': 'OBJECT_ID'}
partner_info_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID', 'pdbBeGeoId': 'PARTNER_BE_GEO_ID',
                        'pdbSiteId': 'PARTNER_SITE_ID', 'countryCd': 'COUNTRY_CD',
                        'contactPaeUSerID': 'PARTNER_CONTACT_PAE_USER_ID', 'dmUpdateDate': 'DM_UPDATE_DATE',
                        'createdOn': 'CREATED_ON', 'updatedOn': 'UPDATED_ON', 'dealSubmittedDate': 'DEAL_SUBMITTED_DATE'}
quote_contacts_columns = {'parentId': 'PARENT_ID', 'contactEmailId': 'CONTACT_EMAIL_ID', 'primaryFlag': 'PRIMARY_FLAG',
                          'contactFirstName': 'CONTACT_FIRST_NAME', 'contactLastName': 'CONTACT_LAST_NAME',
                          'contactPhoneNumber': 'CONTACT_PHONE_NUMBER', 'contactCCOId': 'CONTACT_CCO_ID',
                          'contactJobTitle': 'CONTACT_JOB_TITLE', 'contactURL': 'CONTACT_URL',
                          'otherJobTitle': 'OTHER_JOB_TITLE', 'objectId': 'OBJECT_ID', 'contactType': 'CONTACT_TYPE'}
line_list_columns = {'parentId': 'PARENT_ID', 'objectId': 'OBJECT_ID',
                     'parentLineObjectId': 'PARENT_LINE_OBJECT_ID', 'lineNumber': 'LINE_NUMBER',
                     'quoteObjectId': 'QUOTE_OBJECT_ID', 'lineType': 'LINE_TYPE',
                     'extListPrice': 'EXT_LIST_PRICE',
                     'quantity': 'QUANTITY', 'finalNetPrice': 'FINAL_NET_PRICE',
                     'partNumber': 'PART_NUMBER',
                     'description': 'DESCRIPTION', 'buyMethod': 'FULFILLMENT_TYPE', 'duration': 'DURATION',
                     'contractualDisc': 'CONTRACTUAL_DISC', 'mlbIndicator': 'MLB_INDICATOR',
                     'promoDisc': 'PROMO_DISC',
                     'nonStandardDisc': 'NONSTANDARD_DISC', 'multiYearDisc': 'MULTI_YEAR_DISC',
                     'active': 'ACTIVE', 'tradeInDiscount': 'TRADE_IN_DISC',
                     'createdOn': 'CREATED_DATE', 'createdBy': 'CREATED_BY', 'updatedOn': 'UPDATED_DATE',
                     'updatedBy': 'UPDATED_BY','effectiveDiscStd': 'EFFECTIVE_DISC_STD',
                     'extNetPrice': 'EXT_NET_PRICE_AMT_STD',
                     'tradeInUnitCreditAmt': 'TRADE_IN_UNIT_CREDIT_AMT_STD',
                     'additionalUnitCreditAmt': 'ADDITIONAL_UNIT_CREDIT_AMT',
                     'webOrderId': 'WEB_ORDER_ID', 'orderStatus': 'ORDER_STATUS',
                     'negotiatedAdjustedDisc': 'NEGOTIATED_ADJUSTED_DISC',
                     'additionalItemInfo': 'ADDITIONAL_ITEM_INFO', 'wfPriced': 'WF_PRICED',
                     'solutionIdentifier': 'SOLUTION_IDENTIFIER',
                     'oemRebFlag': 'OEM_REB_FLAG','lineTypeDesc': 'LINE_TYPE_NAME', 'gplListPrice': 'EXT_LIST_PRICE_GPL'}

concession_list_columns = {'objectId': 'OBJECT_ID', 'discountId': 'DISCOUNT_ID',
                           'postTermClauseFlag': 'POST_TERM_CLAUSE_FLAG',
                           'appliedProductFamilyId': 'APPLIED_PRODUCT_FAMILY_ID',
                           'promoVersion': 'PROMO_VERSION',
                           'apprNsDiscountPctg': 'APPR_NS_DISCOUNT_PCTG',
                           'reqNSDiscountPctg': 'REQ_NS_DISCOUNT_PCTG',
                           'modifierLineId': 'MODIFIER_LINE_ID', 'discPercentage': 'DISC_PERCENTAGE',
                           'discAmount': 'DISC_AMOUNT',
                           'priceUsedForAdjustment': 'PRICE_USED_FOR_ADJUSTMENT',
                           'netPriceAfterAdjustment': 'NET_PRICE_AFT_ADJUSTMENT',
                           'pricingPhaseCode': 'PRICING_PHASE_CODE',
                           'modifierLineTypeCode': 'MODIFIEDER_LINE_TYPE_CODE',
                           'listLineNumber': 'LIST_LINE_NUMBER',
                           'includeOnReturnFlag': 'INCLUDE_ON_RETURN_FLAG',
                           'discMethod': 'DISC_METHOD', 'modifierLevelCode': 'MODIFIER_LEVEL_CODE',
                           'bucket': 'BUCKET',
                           'automaticFlag': 'AUTOMATIC_FLAG',
                           'updateAllowableFlag': 'UPDATE_ALLOWABLE_FLAG',
                           'updatedFlag': 'UPDATED_FLAG', 'appliedFlag': 'APPLIED_FLAG',
                           'printOnInvoiceFlag': 'PRINT_ON_INVOICE_FLAG',
                           'changeReasonCode': 'CHANGE_REASON_CODE',
                           'changeReasonText': 'CHANGE_REASON_TEXT',
                           'promotionCode': 'PROMOTION_CODE', 'promotionName': 'PROMOTION_NAME',
                           'channerProgAllignment': 'CHANNER_PROG_ALLIGNMENT',
                           'modifiedFrom': 'MODIFIED_FROM', 'modifiedTo': 'MODIFIED_TO',
                           'estimatedFlag': 'ESTIMATED_FLAG', 'chargeTypeCode': 'CHARGE_TYPE_CODE',
                           'chargeSubtypeCode': 'CHARGE_SUBTYPE_CODE',
                           'rangeBreakQuantity': 'RANGE_BREAK_QUANTITY',
                           'accuralConversionRate': 'ACCURAL_CONVERSION_RATE',
                           'accuralFlag': 'ACCURAL_FLAG',
                           'benefitQuantity': 'BENEFIT_QUANTITY', 'benefitUOMCode': 'BENEFIT_UOM_CODE',
                           'priceBreakTypeCode': 'PRICE_BREAK_TYPE_CODE',
                           'substitutionAttribute': 'SUBSTITUTION_ATTRIBUTE',
                           'prorationTypeCode': 'PRORATION_TYPE_CODE',
                           'creditOrChargeFlag': 'CREDIT_OR_CHARGE_FLAG',
                           'incInSalesPerformance': 'INC_IN_SALES_PERFORMANCE',
                           'rebateTransactionTypeCode': 'REBATE_TRANSACTION_TYPE_CODE',
                           'rebateTransactionReference': 'REBATE_TRANSACTION_REFERENCE',
                           'rebatePaymentSystemCode': 'REBATE_PAYMENT_SYSTEM_CODE',
                           'operandPerQuantity': 'OPERAND_PER_QUANTITY',
                           'adjustedAmtPerQuantity': 'ADJUSTED_AMT_PER_QUANTITY',
                           'listLineId': 'LIST_LINE_ID', 'discountName': 'DISC_NAME',
                           'adjustPriceListAmt': 'ADJUST_PRICE_LIST_AMT',
                           'adjustedAmount': 'ADJUSTED_AMOUNT', 'updatedDate': 'UPDATED_DATE',
                           'updatedBy': 'UPDATED_BY',
                           'creationDt': 'CREATION_DT', 'createdBy': 'CREATED_BY',
                           'modifierHeaderId': 'MODIFIER_HEADER_ID',
                           'quoteLineObjectId': 'QUOTE_LINE_OBJECT_ID', 'concessionId': 'CONCESSION_ID',
                           'concessionType': 'CONCESSION_TYPE',
                           'concessionValue': 'CONCESSION_VALUE',
                           'concessionValueType': 'CONCESSION_VALUE_TYPE',
                           'concessionApplied': 'CONCESSION_APPLIED', 'discountReqVer': 'DISCOUNT_VERSION',
                           'expirationDateAsDate': 'EXPIRATION_DATE'}


def write_csv(columns, data_frame, file_name):
    if not os.path.isfile(create_folder('dynamic') + '/' + file_name) and not os.path.isfile(
            create_folder('static') + '/' + file_name):
        updated_df = data_frame.rename(columns, axis=1)
        return [updated_df.to_csv(create_folder('dynamic') + '/' + file_name, sep='ê', mode='a',
                                  encoding='utf-8', index=False),
                updated_df.to_csv(create_folder('static') + '/' + file_name, sep='ê', mode='a',
                                  encoding='utf-8', index=False)]


def insert_kafka_coulmns(dataframe):
    dataframe['UPDATED_BY_KAFKA'] = ""
    dataframe['UPDATED_ON_KAFKA'] = ""
    dataframe['OFFSET_NUMBER'] = ""
    dataframe['PARTITION_NUMBER'] = ""
    dataframe['MESSAGE_SEQUENCE_NUMBER'] = ""


def create_empty_csv(flag, columns, filename):
    config_properties = collect_property_file_contents('/users/edwadm/CCW_QUOTING_KAFKA/constant.ini', flag)

    df = pd.DataFrame(config_properties.items()).T

    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    insert_kafka_coulmns(df)

    write_csv(columns, df[0:0], filename)


def generate_csv_files():
    create_empty_csv('DEAL', deal_columns, 'st_int_cq_deal_kafka.csv')
    create_empty_csv('ENDCUSTOMER', end_customer_columns, 'st_int_raw_dm_customer_kafka.csv')
    create_empty_csv('DEALNODESDETAILS', deal_node_columns, 'st_int_deal_master_kafka.csv')
    create_empty_csv('PARTYDETAILS', party_details_columns, 'st_int_raw_cq_deal_party_vw_kafka.csv')
    create_empty_csv('NSDEALEXTN', ns_deal_extn_columns, 'st_int_raw_cq_ns_deal_extn_vw_kafka.csv')
    create_empty_csv('DEALREOPENREASON', deal_reopen_reason_columns, 'st_int_deal_reopen_reason_kafka.csv')
    create_empty_csv('TEAMDETAILS', team_details_columns, 'st_int_dmr_dm_team_assignment_kafka.csv')
    create_empty_csv('DEALCONTACTS', deal_contact_columns, 'st_cq_deal_contact_kafka.csv')
    create_empty_csv('COMPETITORDETAILS', competitor_details_columns, 'st_cq_competitor_kafka.csv')
    create_empty_csv('NONSTDTERM', non_std_term_columns, 'st_int_raw_cq_dealnsterm_vw_kafka.csv')
    create_empty_csv('STATETRANSITIONS', state_transitions_columns, 'st_cq_state_transition_int_vw_kafka.csv')
    create_empty_csv('QFRESPONSE', qf_response_columns, 'st_cq_qf_response_kafka.csv')
    create_empty_csv('TECHNOLOGY', technology_columns, 'st_int_raw_pdr_deal_technology_kafka.csv')
    create_empty_csv('QUOTES', quotes_columns, 'st_int_cq_single_quote_vw_kafka.csv')
    create_empty_csv('PARTNERINFO', partner_info_columns, 'st_int_raw_cq_quote_partner_ex_kafka.csv')
    create_empty_csv('BILLINGADDRESS', billing_address_columns, 'st_int_dmr_deal_bill_to_kafka.csv')
    create_empty_csv('QUOTECONTACTS', quote_contacts_columns, 'st_int_raw_pdr_deal_contact_kafka.csv')
    create_empty_csv('DISTIFULFILLMENTDETAILS', disti_fillment_details_columns,
                     'st_int_raw_deal_disti_fulfillm_kafka.csv')
    create_empty_csv('BEHAVIOURREWARDS', behaviour_rewards_columns, 'st_int_raw_cq_behavior_reward.csv')
    create_empty_csv('STANDARDAPPROVALROUTEINFO', standard_approval_route_info_columns,
                     'st_cq_approval_route_kafka.csv')
    create_empty_csv('MAJORLINES', line_list_columns, 'st_int_raw_cq_quote_line_vw_kafka.csv')
    create_empty_csv('CONCESSION', concession_list_columns, 'st_int_raw_cq_quote_lne_conces_kafka.csv')


#generate_csv_files()