import { Injectable } from "@angular/core";
import { QuixService } from "./quix.service";
import { Observable, ReplaySubject, Subject, map, of } from "rxjs";
import { MessagePayload } from "../models/messagePayload";
import { ParameterData } from "../models/parameter-data";

export const QuixChatRoom = 'Quix chatroom';

class RoomChange {
  roomId: string;
  isTwitch: boolean;
}

@Injectable({
  providedIn: "root",
})
export class RoomService {
  selectedRoom: string;
  private _isTwitch: boolean;

  private _roomChanged$ = new Subject<RoomChange>();
  private _previousRooms: string[] = [];
  private _previousRooms$ = new ReplaySubject<string[]>;

  constructor(private quixService: QuixService) {
    this.retrievePreviousRooms();
  }

  private retrievePreviousRooms(): void {
    const localStorageValue = localStorage.getItem('rooms');
    localStorageValue ? JSON.parse(localStorageValue) : [];
    this._previousRooms = localStorageValue ? JSON.parse(localStorageValue) : [];
    this._previousRooms$.next(this._previousRooms);
  }

  private setPreviousRooms(): void {
    localStorage.setItem('rooms', JSON.stringify(this._previousRooms));
    this._previousRooms$.next(this._previousRooms);
  }

  /**
   * Switches the user from one room to another.
   * Conditional based on whether it's a Twitch channel or not.
   * 
   * @param roomName The name of the room.
   * @param isTwitchRoom Whether it's a Twitch channel.
   */
  public switchRoom(roomName?: string, isTwitchRoom?: boolean): void {
    // console.log(`Room Service | Switching room - ${roomName}. Is Twitch - ${isTwitchRoom}`);
    this._isTwitch = !!isTwitchRoom;

    if (roomName && !isTwitchRoom && !this._previousRooms.includes(roomName)) {
      this._previousRooms.push(roomName);
      this.setPreviousRooms();
    }

    roomName = roomName || QuixChatRoom;

    // Unsubscribe from previous room
    if (this.selectedRoom) this.unsubscribeFromRoom(this.selectedRoom)

    // Subscribe to the new room
    this.subscribeToRoom(roomName);

    // Perform room logic
    this.selectedRoom = roomName;
    this._roomChanged$.next({ roomId: this.selectedRoom, isTwitch: this._isTwitch });
  }

  /**
   * Sends a message from the chat. 
   * 
   * @param payload The payload containing various information about the user and message.
   * @param isDraft Whether it is a draft message or not.
   */
  public sendMessage(payload: any, isDraft?: boolean) {
    const topic = isDraft ? this.quixService.draftsTopic : this.quixService.messagesTopic;
    this.quixService.sendParameterData(topic, this.selectedRoom, payload);
  }

  /**
   * Retrieves a list of the last messages for a specific room.
   * 
   * @param roomId The name of the room.
   * @returns An observable containing all the messages.
   */
  public getChatMessageHistory(roomId: string): Observable<MessagePayload[]> {
    let payload =
    {
      'numericParameters': [
        {
          'parameterName': 'sentiment',
          'aggregationType': 'None'
        },
        {
          'parameterName': 'average_sentiment',
          'aggregationType': 'None'
        }
      ],
      'stringParameters': [
        {
          'parameterName': 'chat-message',
          'aggregationType': 'None'
        }
      ],

      'streamIds': [
        roomId
      ],
      'groupBy': [
        'role',
        'name',
        'profilePic',
        'profilePicColor'
      ],
    };
    return this.quixService.retrievePersistedParameterData(payload).pipe(
      map(rows => {
        let results: MessagePayload[] = [];

        rows.timestamps.forEach((timestamp, i) => {
          let name = rows.tagValues['name']?.at(i)!;
          let profilePic = rows.tagValues['profilePic']?.at(i)
          let profilePicColor = rows.tagValues['profilePicColor']?.at(i)
          let sentiment = rows.numericValues['sentiment']?.at(i) || 0;
          let value = rows.stringValues['chat-message']?.at(i);
          let message = results.find((f) => f.timestamp === timestamp && f.name === name);

          if (message) {
            message.value = value;
            message.sentiment = sentiment;
          } else {
            results.push({
              timestamp,
              value,
              sentiment,
              name,
              profilePic,
              profilePicColor
            })
          }
        })

        return results;
      })
    )
    
  }

  /**
   * Retrieved the last sentiments for the room from the last 5 minutes
   */
  public getChatSentimentHistory(roomId: string): Observable<ParameterData> {
    // Calculate the timestamp of 5 mins ago in nanoseconds
    const currentTimeInNano = Date.now() * 1e6;
    const fiveMinutesInNano = 5 * 60 * 1e9;
    const fiveMinutesAgoInNano = currentTimeInNano - fiveMinutesInNano;

    let payload = 
    {
        'topic': 'chat-with-sentiment',
        'groupByTime': {
            'timeBucketDuration': 7791291230,
            'interpolationType': 'None'
        },
        'from': fiveMinutesAgoInNano,
        'numericParameters': [
            {
                'parameterName': 'average_sentiment',
                'aggregationType': 'Mean'
            }
        ],
        'stringParameters': [],
        'binaryParameters': [],
        'eventIds': [],
        'eventAggregation': {
            'interpolationType': 'None',
            'interval': 7791291230,
            'aggregationType': 'First'
        },
        'streamIds': [
            roomId
        ],
        'tagFilters': []
    };
    return this.quixService.retrievePersistedParameterData(payload);
  }

  /**
   * Subscribes to all the relevant topics for a specific room.
   * 
   * @param roomName The name of the room we are joining.
   */
  public subscribeToRoom(roomName: string): void {
    // console.log(`Room Service | Subscribing to the room - ${roomName}`);
    this.quixService.subscribeToParameter(this.quixService.messagesTopic, roomName, "*");
    this.quixService.subscribeToParameter(this.quixService.twitchMessagesTopic, roomName, "*");
    this.quixService.subscribeToParameter(this.quixService.draftsTopic, roomName, "*");
    this.quixService.subscribeToParameter(this.quixService.sentimentTopic, roomName, "*");
    this.quixService.subscribeToParameter(this.quixService.draftsSentimentTopic, roomName, "*");
  }

  /**
   * Unsubscribes from all the relevant topics for a specific room.
   * 
   * @param roomName The name of the room we are leaving. 
   */
  public unsubscribeFromRoom(roomName: string): void {
    // console.log(`Room Service | Unsubscribing from the room - ${roomName}`);
    this.quixService.unsubscribeFromParameter(this.quixService.messagesTopic, roomName, "*");
    this.quixService.unsubscribeFromParameter(this.quixService.twitchMessagesTopic, roomName, "*");
    this.quixService.unsubscribeFromParameter(this.quixService.draftsTopic, roomName, "*");
    this.quixService.unsubscribeFromParameter(this.quixService.sentimentTopic, roomName, "*");
    this.quixService.unsubscribeFromParameter(this.quixService.draftsSentimentTopic, roomName, "*");

  }

  get roomChanged$() {
    return this._roomChanged$.asObservable();
  }

  get previousRooms$(): Observable<string[] | undefined> {
    return this._previousRooms$.asObservable() || of();
  }
}
