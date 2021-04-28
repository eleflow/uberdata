//package eleflow.uberdata.model
//
//
//
//
//import scala.annotation.tailrec
//
///**
// * Created by dirceu on 31/03/17.
// */
//
//
//trait TabbedToString {
//	_: Product =>
//	override def toString: String = excludeSome(productIterator.mkString(",").replaceAll("None",
//		"null"))
//
//	@tailrec
//	private def excludeSome(s: String): String = {
//		val someIdx = s.indexOf("Some(")
//		if (someIdx < 0) {
//			s
//		} else {
//			val endIdx = s.indexOf(")", someIdx)
//			excludeSome(s.substring(0, someIdx + 5) + s.substring(endIdx + 1))
//		}
//	}
//}
//
//case class PartialCallEndModule(failoverCorrelationId: String) extends TabbedToString
//
//case class PartialCallBeginModule(failoverCorrelationId: String)
//	extends TabbedToString
//
//case class CallingPartyAddress(callingPartyAddress: String) extends TabbedToString
//
//case class EarlyMedia(sdpOfferTimestamp: String,
//											sdpAnswerTimestamp: String,
//											earlyMediaSdp: String,
//											earlyMediaInitiatorFlag: String) extends TabbedToString
//
//case class MessageBody(bodycontentType: String,
//											 bodyContentLength: String,
//											 bodyContentDisposition: String, bodyOriginator:
//											 String) extends TabbedToString
//
//case class TgppModule(primaryDeviceLinePort: String,
//											calledAssertedIdentity: String,
//											calledAssertedPresentationIndicator: String,
//											sdp: String, mediaInitiatorFlag: String,
//											earlyMediaList: Array[EarlyMedia],
//											messageBodyList: Array[MessageBody],
//											sipErroCode: String,
//											callingPartyAddressList: Array[CallingPartyAddress])
//	extends TabbedToString
//
//case class CorrelationInfo(key: String, creator: String,
//													 originatorNetwork: String,
//													 terminatorNetwork: String,
//													 otherInfoInPCV: String) extends TabbedToString
//
//case class IpModule(route: String, networkCallID: String,
//										coded: String, accessDeviceAddress: String,
//										accessCallID: String, accessNetworkInfo: String,
//										correlationInfo: CorrelationInfo,
//										chargingFunctionAddress: String,
//										codecUsage: String, routingNumber: String,
//										pCamelLocInfo: String,
//										pCamelMscAddress: String,
//										pCamelCellIDorLAI: String, userAgent: String,
//										gets: String) extends TabbedToString
//
//case class Location(location: String, locationType: String)
//	extends TabbedToString
//
//
//case class Host(group: String, userId: String,
//								userNumber: String,
//								groupNumber: String) extends TabbedToString
//
//case class ServiceExtension(serviceName: String,
//														invocationTime: String, facResult: String,
//														host: Host, pushToTakl: String,
//														relatedCallId: String,
//														mediaselection: String,
//														action: String, resulttype: String,
//														startTime: String, stoptime: String,
//														confid: String,
//														locationActivationResult: String,
//														locationDeactivationResult: String,
//														callretrieveResult: String,
//														charge: String,
//														currency: String, time: String,
//														summ: String, callBridgeResult: String,
//														nightServiceActivationMOResult: String,
//														nightServiceDeactivationMOResult: String,
//														forcedForwardingActivationResult: String,
//														forcedForwardingDeactivationResult: String,
//														outgoingCallCenterCallFACResult: String,
//														outgoingPersonalCallFACResult: String,
//														outgoingCallCenterPhoneNumber: String,
//														outgoingCallCenterUserId: String,
//														outgoingCallCenterGroupNumber: String,
//														routingNumber: String,
//														preAlertingDuration: String,
//														conferenceId: String,
//														role: String, bridge: String,
//														owner: String, ownerDN: String,
//														title: String,
//														projectCode: String,
//														recordingDuration: String,
//														transactionId: String,
//														mobilityNumber: String,
//														mobilityRoutingNumber: String,
//														recordingTrigger: String,
//														recordingDestination: String,
//														recordingResult: String,
//														sccCallid: String, sccNumber: String,
//														sccCause: String, targetHungGroupId: String,
//														flexibleSeatingHost: String) extends TabbedToString
//
//case class CenterxModule(group: String, department: String,
//												 accountCode: String,
//												 authorizationCode: String,
//												 cbfAuthorizationCode: String,
//												 callingPartyCategory: String,
//												 outsideAccessCode: String,
//												 originalCalledNumber: String,
//												 originalCalledNumberContext: String,
//												 originalCalledPresentationIndicator: String,
//												 originalCalledReason: String,
//												 redirectingNumber: String,
//												 redirectingNumberContext: String,
//												 redirectingPresentationIndicator: String,
//												 redirectingReason: String,
//												 trunkGroupName: String,
//												 trunkGroupInfo: String, chargeNumber: String,
//												 relatedCallId: String,
//												 relatedCallIdReason: String,
//												 faxMessaging: String,
//												 twoStageDiallingDigits: String,
//												 recallType: String,
//												 originationMethod: String,
//												 serviceExtensionList: Array[ServiceExtension],
//												 prepaidStatus: String,
//												 configurableCLID: String,
//												 virtualOnNetType: String,
//												 officeZone: String, primaryZone: String,
//												 roamingMscAddress: String,
//												 customSchemaVersion: String,
//												 locationList: Array[Location],
//												 locationUsage: String, cicInsertedAsCac: String
//												 , extTrackingId: String) extends TabbedToString
//
//case class RecordId(eventCounter: String, systemId: String, date:
//String, systemTimeZone: String) extends TabbedToString
//
//case class HeaderModule(recordId: RecordId, serviceProvider: String, tp: String)
//	extends TabbedToString
//
//case class BasicModule(userNumber: String, groupNumber: String,
//											 asCallType: String, callingNumber: String,
//											 callingNumberContext: String,
//											 callingPresentationNumber: String,
//											 callingPresentationNumberContext: String,
//											 callingAssertedNumber: String,
//											 callingAssertedNumberContext: String,
//											 dialedDigits: String,
//											 dialedDigitsContext: String,
//											 calledNumber: String,
//											 calledNumberContext: String,
//											 networkTranslatedNumber: String,
//											 networkTranslatedNumberContext: String,
//											 networkTranslatedGroup: String,
//											 startTime: String, userTimeZone: String,
//											 localCallId: String, remoteCallId: String,
//											 answerIndicator: String,
//											 answerTime: String, releaseTime: String,
//											 terminationCause: String,
//											 carrierIdentificationCode: String,
//											 callCategory: String, networkCallType: String,
//											 chargeIndicator: String, typeOfNetwork: String,
//											 releasingParty: String,
//											 userId: String, otherPartyName: String,
//											 otherPartyNamePresentationIndicator: String,
//											 clIdPermitted: String,
//											 receivedCallingNumber: String, namePermitted:
//											 String) extends TabbedToString
//
//case class CDR(headerModule: HeaderModule, basicModule: BasicModule,
//										 centerxModule: CenterxModule, ipModule: IpModule,
//										 tgppModule: TgppModule,
//										 partialCallBeginModule: PartialCallBeginModule,
//										 partialCallEndModule: PartialCallEndModule) extends TabbedToString