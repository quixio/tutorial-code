import { Component, ElementRef, OnInit, ViewChild, Pipe, PipeTransform } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { Subject, Subscription, debounceTime, filter, take, takeUntil, timer } from 'rxjs';
import { MessagePayload } from 'src/app/models/messagePayload';
import { ParameterData } from 'src/app/models/parameterData';
import { ConnectionStatus, QuixService } from 'src/app/services/quix.service';
import { Animals } from '../../constants/animals';
import { TitleCasePipe } from '@angular/common';
import { MatDialog } from '@angular/material/dialog';
import { ShareChatroomDialogComponent } from '../dialogs/share-chatroom-dialog/share-chatroom-dialog.component';
import { RoomService } from 'src/app/services/room.service';
import { Colors } from 'src/app/constants/colors';

export const POSITIVE_THRESHOLD = 0.5;
export const NEGATIVE_THRESHOLD = -0.5;

export class UserTyping {
  timeout?: Subscription;
  sentiment?: number;
}


/**
 * Custom Pipe to filter the list of messages, so that
 * it only shows the messages in the chat where a sentiment
 * is present.
 */
@Pipe({
  name: 'sentimentFilter'
})
export class SentimentFilterPipe implements PipeTransform {
  transform(messages: MessagePayload[], isTwitch: boolean): any[] {
    if (!messages || !isTwitch) {
      return messages;
    }

    // Filter the messages to only show the ones where sentiment is present
    return messages.filter(message => message.sentiment !== undefined);
  }
}

@Component({
  selector: 'app-web-chat',
  templateUrl: './web-chat.component.html',
  styleUrls: ['./web-chat.component.scss'],
  providers: [TitleCasePipe]
})
export class WebChatComponent implements OnInit {
  username: string;
  profilePic: string;
  profilePicColor: string;

  isTwitch: boolean = true;
  scrollTimeoutId: any;

  @ViewChild('chatWrapper') chatWrapperEle: ElementRef<HTMLElement>;
  @ViewChild('messageInput') messageInputEle: ElementRef;

  connectionState = ConnectionStatus;
  readerConnectionStatus: ConnectionStatus = ConnectionStatus.Offline;
  writerConnectionStatus: ConnectionStatus = ConnectionStatus.Offline;

  usernameFC = new FormControl('', Validators.required);
  messageFC = new FormControl("");
  chatForm = new FormGroup({
    message: this.messageFC,
  });
  
  messages: MessagePayload[] = [];
  happyTypers = new Set<string>();
  unhappyTypers = new Set<string>();
  usersTyping: Map<string, UserTyping> = new Map();
  averageSentiment: number = 0;
  typingTimeout: number = 4000;
  typingDebounce: number = 300;
  messageSent: string | undefined;
  draftGuid: string | undefined;

  private unsubscribe$ = new Subject<void>();
  
  constructor(public quixService: QuixService, public roomService: RoomService, private titleCasePipe: TitleCasePipe, private matDialog: MatDialog) { }

  ngOnInit(): void {
    this.profilePic = this.generateProfilePic();
    this.profilePicColor = this.generateRandomColor();

    this.messageFC.valueChanges.pipe(debounceTime(300), takeUntil(this.unsubscribe$)).subscribe((value) => {
      // Prevents it triggering when they send message and debounce is triggered
      if (this.messageSent === value) {
        this.messageSent = undefined;
        return;
      }
      
      // Generate a new GUID if there isn't one already or if they clear the input
      if (!this.draftGuid || value === '') this.draftGuid = this.generateGUID();
      this.sendMessage(true);
    });

     // Listen for connection status changes
     this.quixService.readerConnStatusChanged$.pipe(takeUntil(this.unsubscribe$)).subscribe((status) => {
      this.readerConnectionStatus = status;
    });
    this.quixService.writerConnStatusChanged$.pipe(takeUntil(this.unsubscribe$)).subscribe((status) => {
      this.writerConnectionStatus = status
    });
  
    // Listen for reader messages
    this.quixService.paramDataReceived$.pipe(
      takeUntil(this.unsubscribe$), 
      filter((f) => f.streamId === this.roomService.selectedRoom) // Ensure there is no message leaks
    ).subscribe((payload) => {
      this.messageReceived(payload);
    });

    this.roomService.roomChanged$.subscribe(({ roomId, isTwitch }) => {
      this.messages = [];
      this.isTwitch = isTwitch;

      if (!isTwitch) {
        this.roomService.getChatMessageHistory(roomId).pipe(take(1)).subscribe(lastMessages => {
          let sortedMessages = lastMessages.sort((a, b) => a.timestamp - b.timestamp);
          this.messages = sortedMessages;
          this.scrollToChatBottom();
        });
      }
    });
  }

  submit(): void {
    if (!this.username) {
      this.username = this.usernameFC.value!;
      this.scrollToChatBottom();
      setTimeout(() => this.messageInputEle.nativeElement.focus(), 0);
      return;
    }

    this.sendMessage(false);
    this.messageSent = this.messageFC.value!;
    this.messageFC.reset("", { emitEvent: false });
  }

  private sendMessage(isDraft: boolean): void {
    const message: string = this.messageFC.value || "";
    if (!isDraft) this.draftGuid = undefined;

    const payload = {
      timestamps: [new Date().getTime() * 1000000],
      tagValues: {
        room: [this.roomService.selectedRoom],
        role: ['Customer'],
        name: [this.username],
        profilePic: [this.profilePic],
        profilePicColor: [this.profilePicColor],
        draft_id: [this.draftGuid]
      },
      stringValues: {
        "chat-message": [message],
      }
    };

    this.roomService.sendMessage(payload, isDraft);
  }

  private messageReceived(payload: ParameterData): void {
    let topicId = payload.topicId;
    let [timestamp] = payload.timestamps;
    let [name] = payload.tagValues["name"];
    let profilePic = payload.tagValues["profilePic"]?.at(0);
    let profilePicColor = payload.tagValues["profilePicColor"]?.at(0);
    let sentiment = payload.numericValues["sentiment"]?.at(0) || undefined;
    let averageSentiment = payload.numericValues["average_sentiment"]?.at(0) || 0;
    let value = payload.stringValues["chat-message"]?.at(0);
    let message = this.messages.find((f) => f.timestamp === timestamp && f.name === name);
    let user = this.usersTyping.get(name);


    if (topicId === this.quixService.draftsTopic) {
      const timer$ = timer(3000);
      // If they were already tying
      if (user) this.usersTyping.get(name)?.timeout?.unsubscribe();
      // When it finishes, removes the user from typing list
      const subscription = timer$.subscribe(() => {
        this.usersTyping.delete(name);
      })
      // Add the subscription to the object
      this.usersTyping.set(name, {
        ...user,
        timeout: subscription
      });
    }

    if (topicId === this.quixService.draftsSentimentTopic) {
      if (!user || !sentiment) return;
      user = { ...user, sentiment };
      if (sentiment > 0) this.happyTypers.add(name);
      if (sentiment < 0) this.unhappyTypers.add(name);
      this.usersTyping.set(name, user);
    }

    if (topicId === this.quixService.messagesSanitizedTopic || topicId === this.quixService.twitchMessagesTopic) {
       // If the user is in the typing map then remove them
       if (user) {
        user?.timeout?.unsubscribe();
         this.usersTyping.delete(name);
       } 
       // Push the new message
       this.messages.push({timestamp, name, profilePic, profilePicColor, sentiment, value });
    }

    if (topicId === this.quixService.sentimentTopic) {
      if (!message) return;

      // Update existing message with the sentiment
      message.sentiment = sentiment;
      this.averageSentiment = averageSentiment;
      
      // Trigger change detection for the pipe
      this.messages = [...this.messages];
    }

    // Scroll to the button of the chart
    this.scrollToChatBottom(true);
  }

  /**
   * Based on the users typing in chat, it generates statistics in the 
   * sense of faces (Smiley, Neutral and Frowney) on the sentiment of
   * the users draft messages.
   * 
   * @returns The statistics from the sentiment.
   */
  public getSentimentStats(): { smileys: string[], neutrals: string[], frowneys: string[] } {
    let smileys: string[] = [];
    let neutrals: string[] = [];
    let frowneys: string[] = [];
    this.usersTyping.forEach(({sentiment}, name) => {
      if (sentiment! > POSITIVE_THRESHOLD) smileys.push(name);  
      if (sentiment! < POSITIVE_THRESHOLD && sentiment! > NEGATIVE_THRESHOLD) neutrals.push(name);  
      if (sentiment! < NEGATIVE_THRESHOLD) frowneys.push(name);  
    });

    return { smileys, neutrals, frowneys };
  }

  /**
   * Based on the users typing in the chat, it generates the average sentiment
   * of all their draft messages. This can then be displayed on the typing message.
   * 
   * @returns An average of all the userTyping sentiments.
   */
  public getDraftSentimentAverage(): number {
    // Get all the sentiments for user's currently typing
    const sentimentsArray = [...this.usersTyping.values()].map(userTyping => userTyping.sentiment || 0);
    // Calculate the sum of all numbers
    const sum = sentimentsArray.reduce((acc, current) => acc + current, 0);
    // Calculate the mean (average)
    const mean = sum / sentimentsArray.length;

    return mean;
  }


  /**
   * Based on how many users are typing, it generates the appropriate
   * isTyping message to be displayed in the template.
   * 
   * @returns The Html message.
   */
  public getTypingMessage(): string | undefined {  

    const users = Array.from(this.usersTyping.entries())
    .map(([key, value]) => ({
      name: this.titleCasePipe.transform(key),
      sentiment: value.sentiment!,
    }));

    if (!users.length) return undefined;

    if (users.length === 1) {
      const [user] = users;
      return `<b>${user.name}</b> is typing...`;
    }

    if (users.length < 3) {
      const usersJoined = users.map((m) => m.name).join(' and ');
      return `<b>${usersJoined}</b> are typing...`; 
    }

    if (users.length >= 3) {
      return `<b>${users.length}</b> users are typing...`;    
    }

    return undefined
  }

  /**
   * Util method for generating a v4 GUID.
   * 
   * @returns The generated GUID.
   */
  private generateGUID(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
  }

  /**
   * Util method for generating a random profile pic from
   * a list of images.
   * 
   * @returns The random profile pic.
   */
  private generateProfilePic(): string {
    const randomNumber = Math.floor(Math.random() * Animals?.length);
    return `https://ssl.gstatic.com/docs/common/profile/${Animals[randomNumber]}_lg.png`;
  }

  /**
   * Util method for generating a random color from
   * a list of colors.
   *
   * @returns The random color. 
   */
  private generateRandomColor(): string {
    const randomNumber = Math.floor(Math.random() * Colors?.length);
    return Colors[randomNumber];
  }

  /**
   * Only needed for mobile users.
   * Scrolls the info section into view.
   */
  scrollToInfo(): void {
    const chatEle = document.getElementById('info-section');
    chatEle?.scrollIntoView({
      behavior: 'smooth'
    })
  }
  
  /**
   * Util method to scroll the user to the bottom of the chat
   */
  scrollToChatBottom(isCheckBottom?: boolean): void {
    const threshold = 30;
    const el = this.chatWrapperEle.nativeElement;
    const isScrollToBottom = Math.round(el.scrollTop + el.clientHeight) >= el.scrollHeight - threshold;

    if (isCheckBottom && !isScrollToBottom) return;
  
    // Clear the existing timeout if it exists
    if (this.scrollTimeoutId) clearTimeout(this.scrollTimeoutId);
    this.scrollTimeoutId = setTimeout(() => el.scrollTop = el.scrollHeight, 0);
  }

  openShareChatroomDialog(): void {
    this.matDialog.open(ShareChatroomDialogComponent, {
      maxWidth: '480px',
      autoFocus: false
    });
  }

  ngOnDestroy(): void {
    this.unsubscribe$.next();
    this.unsubscribe$.complete();
  }

}
