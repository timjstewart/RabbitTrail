import Prelude hiding (catch)

import System.Environment
import Network.AMQP
import System.Exit
import Control.Exception
import qualified Data.ByteString.Lazy.Char8 as B

data ConnParams = ConnParams {
     host        :: String,
     virtualHost :: String,
     userName    :: String,
     password    :: String
} deriving Show

parseArgs :: IO (Either String ConnParams)
parseArgs = do
          args <- getArgs
          case args of
               [h, v, n, p] -> return $ Right $ ConnParams h v n p
               _            -> return $ Left $ "invalid arguments"

usage :: String -> IO ()
usage error = do
      putStrLn $ "error: " ++ error
      putStrLn "usage: runghc RabbitTrail.hs HOST VIRTUAL_HOST USER_NAME PASSWORD"
      exitWith $ ExitFailure 1

run :: ConnParams -> IO ()
run p = do 
    (conn,chan) <- connect p
    showAllMessages chan
    getLine
    closeConnection conn

connect :: ConnParams -> IO (Connection, Channel)
connect p = do 
    conn <- openConnection (host p) (virtualHost p) (userName p) (password p)
    chan <- openChannel conn
    return (conn, chan)

showAllMessages :: Channel -> IO ()
showAllMessages chan = do
    showMessages chan "error"
    showMessages chan "warning" 
    showMessages chan "info" 

anonymousQueue = QueueOpts "" False False True True

showMessages :: Channel -> String -> IO ()
showMessages chan queueName = do
             putStrLn $ "listening to " ++ queueName ++ " queue."
             (queue, _, _) <- declareQueue chan anonymousQueue
             bindQueue chan queue "amq.rabbitmq.log" queueName
             consumeMsgs chan queue Ack (callback queueName)
             return ()
              
callback :: String -> (Message, Envelope) -> IO ()
callback queueName (msg, envelope) = do
         putStrLn $ queueName ++ ": " ++ (B.unpack $ msgBody msg)
         ackEnv envelope

main = do
     args <- parseArgs 
     case args of
          Left(error) -> usage error
          Right(connParams) -> do
               result <- (run connParams)
               return ()
