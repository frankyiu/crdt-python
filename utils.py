
import asyncio


class BroadCaster:
    """ Simple BroadCaster for local testing
    """
    def __init__(self):
        self.listeners =[]
    
    def register_listener(self, listener):
        self.listeners.append(listener)
    
    def broadcast(self, message, sender_id):
        # asyncio.create_task(self.broadcast_task(message, sender_id))
        self.broadcast_task(message, sender_id)

    def broadcast_task(self, message, sender_id):
        for listener in self.listeners:
            if listener.site_id != sender_id:
                listener.receive(message)
    
    # async def broadcast_task(self, message, sender_id):
    #     tasks = []
    #     for listener in self.listeners:
    #         if listener.site_id != sender_id:
    #             tasks.append(listener.receive(message))
    #     await asyncio.gather(*tasks)  

    
    

        