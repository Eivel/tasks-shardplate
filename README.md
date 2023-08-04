The application consists of 3 main modules:
 - Worker Pool (internal/workerpool) - The mail module to store the logic of worker pools. I focused in my logic to generalize the module as much as possible, allowing for custom worker functions and finalizations, custom elements sent to the channels.
 - Queue (internal/queue) - A PoC implementation of a queue with a simple producer and a simple element reservation logic. The element is first reserved and then removed only after successful confirmation.
 - HTTP Server (cmd/server) - Use of 3rd party solution, Echo.

 Along with them, there are several smaller modules to help satisfy business needs:
 - Buffer (internal/buffer) - Responsible for buffering the requests before storing them in disk by batches. Used both inside the worked function and worker finalization function.
 - Payload (internal/payload) - Stores the structure of the payload used in HTTP requests along with the interface logic for the Identifiable resource (allowing for our structure changes in the future with backwards compatibility).
 - Stats (internal/stats) - Used to calculate the number of processed requests for each worker and to store it to disk.
 - Storage (internal/storage) - A localstorage implementation of the io.Writer to store workers work to disk.

 The task of proper, general implementation of the modules (so in the future the structural changes don't impact too many places in the app) was bigger than I expected, so I decided to stop before finishing everything.

 I took the following shortcuts:
 - Test Suite - I covered only workerpool, storage and stats to showcase my approach to testing and how I would approach the queue, buffer, etc. All the modules I designed are self-sufficient, decoupled, so testing them in unit tests is pretty easy. For integration tests, I'd test the general behavior of the few connected modules: http server -> queue -> workerpool, with several combinations of smaller modules.
 - HTTP Server - currently it's basically a "hello world" tier of the server implementation, no verification is done and there are no custom errors handled. I decided to focus more on the proper implementation of the worker pool, the queue and the whole flow working together. I didn't have time to prepare the server more. It works for the showcase, but needs lots of development. I chose Echo as my 3rd party lib, because of its easy handling of TLS certs.
 - I provided some configuration for the worker pools or the queue, but the topic is still not explored enough.
 - I simplified the "graceful shutdown" flow. In the real world scenario I'd need to implement a safe way to store unprocessed data into persistent storage (for example, store the queue elements that weren't processed by workers in time)

 So to summarize the listed tips:
 1. Design - I used the popular pattern for applications with many binaries - using cmd/ subfolders for each main.go and its logic. The shared components are kept in the internal/ path.
 2. Clear over clever - I admit I sometimes try to be too clever with some designs. I hoped to create as much generalization for resources as possible, accepting the interfaces whenever it was possible and improved the usability and readability.
 3. Testability and tests - as I mentioned, I created only some tests as a showcase. It's far from the proper coverage of the project.
 4. API footprint - I haven't explored this part that much, as I left the server for the last and didn't develop it much.
 5. Shipping the app to cloud environment - It was the big part of my focus in this task. I designed the server, queue and worker pools parts to be completely decoupled, connected only via exposed methods or channels. This way we can scale them separately, depending on the business needs.
 6. Protecting your app in the public internet - I haven't touched this part yet in my implementation. Ideally I'd use short-lived tokens with proper token regeneration flow, after initial authentication in the system. Only the HTTP part of the application would be exposed to the public internet, the queue and workers would lie in the private subnetwork.
 7. Further enhancements - I think that in this case my queue or the way to handle interfaces would qualify. I tried to write almost everything myself, using almost entirely the standard lib.

 The entrypoint for this app is cmd/server/main.go, where an example queue, worker pools and http server are created. The implementation handles interrupt signal to exit everything gracefully.
