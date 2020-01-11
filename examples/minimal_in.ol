include "console.iol"
include "json_utils.iol"

// This example runs on outbound messages and allows for adding eg. an signature.

execution { single } // Mandatory

// Input:
// {
//   "messageBody": <MESSAGE_BODY>,
//   "ownID": <USER_ID>,
//   "senderID": <USER_ID>,
//   "recipientIDs": [<USER_ID>, ...]
// }


type InputType: void {
    .messageBody: string
    .ownID: int
    .senderID: int
    .recipientIDs[1, *]: int 
}

// Output: 
// {
//   "action": <ACTION>,
//   "reply": [
//     {
//       "to": <USER_ID>,
//       "message": <MESSAGE_BODY>
//     },
//     ...
//   ]
// }

type actionType: void {
    ."forward": string
    ."drop": string
}

type replyType: void {
    .to: int
    .message: string
}

type OutputType: void {
    .action: string
    .reply[0,*]: replyType
}

main {
    actions = {.forward = "FORWARD", .drop = "DROP"}
    println@Console( "Input: \n" + args[0] )()
    getJsonValue@JsonUtils(args[0])(message)

    if (message instanceof InputType) {
        out.action = "FORWARD"
        out.reply[0] = null // Don't generate any reply messages
        getJsonString@JsonUtils(out)(encoded)
        println@Console( encoded )(  )
    } else {
        getJsonString@JsonUtils( {.action = "FORWARD", .reply[0] = null} )( encoded )
        println@Console( encoded )(  )
    }
}
