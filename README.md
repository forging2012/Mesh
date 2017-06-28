# Mesh 

Is a service discovery using the DHT of `go-libp2p`. 

- Publish Funcions:
    To pulish a function ``` func(X proto.Message) (proto.Message,error) ```, The types of the  argument and the result has to implement the proto.Message  interface (```github.com/gogo/protobuf/proto```)

- Simple Queues Consumers: 
    To publish a consumer ``` func(Msg proto.Message) error```  using queue like in Redis and RabbitMq. 


## Usage


- Functions

  Publish Function: 
    ``` go
    f1:=func (x proto.Message) (proto.Message, error) {
    	r1 := x.(*protocol.X)	    	    		            
	    return  &protocol.FnX{
		    MsgR: "Result of " + r1.Msg,
	    },nil	    
    }

    node := Mesh.NewNode(9200)
    node.BootstrapDHT(1 * time.Second)
    addrs:=node.GetAddresses()
    fmt.Println(addrs)
    node.PublishFunction("f1", f1, &protocol.X{}, &protocol.FnX{})
    ```
    Call Function:
   
    ``` go 
    node:=Mesh.NewNode(9301, "Address of one node in the mesh")
    node.BootstrapDHT(1 * time.Second)
    fn:=GetPublishedFunction("f1", &protocol.X{}, &protocol.FnX{})        
    result,error:=fn(&protocl.X{Msg: "TEST"})
    if error!=nil{
        fmt.Println(error)
    }
    fmt.Println(result)
    ```

- Simple Queues Consumers:

    Consumer
    ```go 
    node := Mesh.NewNode(9200)
    node.BootstrapDHT(1 * time.Second)

    consumer:=func(x proto.Message) error {
	    r1 := x.(*protocol.X)
		fmt.Prinln("Consuming something: ", r1.GetMsg())
		return nil
	}

    config := Config.ConsumerConfig{
    	Retry:       2,     //tries after send the message to dlq queue
	   	ConsumeDL:   true,  //try to consume from dlq queue 
	    CheckDL:     5 * time.Second, // check dlq queue to consume  
	    NoGorutines: 5,     // Routines with the same consumer 
	}
     	        
    cli:=QueueClients.NewRedisClient("localhost:6379")
    erInAdC := nodei.AddConsumer("queue1", consumer, &protocol.X{}, config, cli)
    ```    

    Publish Messagess:
    ```go
    node:=Mesh.NewNode(9301, "Address of one node in the mesh")
    node.BootstrapDHT(5 * time.Second)

    publisFn:=nodeNplus1.PublisherFunction("queue1", &protocol.X{})

    error:=publisFn(&protocol.X{Msg: "NewMessage"})
    if error!=nil{
        fmt.Println()
    }    
    ```

## TODO

    - Test
    - Pub/Sub
    - Key/Value 