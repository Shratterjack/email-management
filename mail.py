from apiclient import errors
import mysql.connector
import base64
import time
import re


class Mail:
    def __init__(self,conn,db):
        self.conn = conn,
        self.db = db


# request to fetch messages from gmail account ,containing only message id
    def getMessages(self,userId='me'):
        service = self.conn[0]
        messages = []
        try:
            messagesObj = service.users().messages().list(userId=userId, maxResults=1000).execute()
            if 'messages' in messagesObj:
                messages = messagesObj.get("messages", [])
            
            while 'nextPageToken' in messagesObj:
                messagesObj = service.users().messages().list(userId=userId, maxResults=1000, pageToken=messagesObj['nextPageToken']).execute()
                newMessages = messagesObj.get("messages", [])
                messages.extend(newMessages)

            return messages

        except errors.HttpError as err:
            print('An error occurred:',err)

# extracts the sender email id and sender name
    def extractSenderMail(self,value):
        sender = {}
        leftIndex = value.find('<')
        if(leftIndex != -1):
            rightIndex = value.find('>')
            sender['name'] = value[0:leftIndex].strip()
            sender['emailId'] = value[leftIndex+1:(rightIndex)]
        else:
            sender['name'] = value.strip()
            sender['emailId'] = value
        
        return sender

# cleans a string by removing unicode characters, special characters and trailing spaces
    def cleanTextValue(self,value):
        temp_value = re.sub(r'[^\x00-\x7F]+', ' ', value)
        final_value = re.sub(r'\W+', ' ', temp_value).strip()
        return final_value

# callback given to batch request to fetch necessary data fields for messages 
# and insert them into database
    def ingestData(self, request_id, response, exception):
        if exception is None:
            final_data = {}
            senderArray = [inner for inner in response['payload']['headers'] if inner['name'] == "From"]
            subjectArray = [inner for inner in response['payload']['headers'] if inner['name'] == "Subject"]
            sender = self.extractSenderMail(senderArray[0]['value'])
            final_data['id'] = response['id']
            final_data['threadId'] = response['threadId']
            final_data['labelIds'] = response['labelIds']
            final_data['subject'] =  self.cleanTextValue(subjectArray[0]['value'])
            final_data['senderName'] = self.cleanTextValue(sender['name'])
            final_data['senderId'] = sender['emailId']
            final_data['snippet'] = self.cleanTextValue(response['snippet'])
            final_data['historyId'] = response['historyId']
            final_data['date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(response['internalDate'])/1000))

            # inserts the data from messages into the database
            try:
                mycursor = self.db.cursor()

                sql = "INSERT INTO mailHistory (id, threadId,labelIds,subject,senderName,senderId,snippet,historyId,date) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)"

                val = (final_data['id'], final_data['threadId'], ','.join(final_data['labelIds']), final_data['subject'], final_data['senderName'], final_data['senderId'],final_data['snippet'],final_data['historyId'], final_data['date'])

                mycursor.execute(sql, val)
                result = self.db.commit()
                print(mycursor.lastrowid, "record inserted.")

            except mysql.connector.Error as err:
                print(err)
          
        else:
            print(exception)

#  executes batch requests to fetch messages for each message id
    def executeRequests(self):
        messageData = []
        data = []
        messages = self.getMessages()
        messageCount = len(messages)
        i = 0
        service = self.conn[0]
        batch = service.new_batch_http_request(callback=self.ingestData)
        while i < messageCount:
            if i%100 == 0:
                batch.execute()
                batch = service.new_batch_http_request(callback=self.ingestData)
            batch.add(service.users().messages().get(userId='me', id=messages[i]['id'],format='full'))
            i = i + 1
 


