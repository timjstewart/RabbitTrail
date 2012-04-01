{-# LANGUAGE ScopedTypeVariables #-}

module RabbitTrail (main) where

import Prelude hiding (catch, log)
import System.Environment (getArgs)
import System.IO (hFlush, hPutStrLn, stdout)
import System.Exit (ExitCode(..), exitWith)
import Control.Exception (IOException, try)
import Control.Concurrent (forkIO)

import Control.Concurrent.MVar (
       MVar, 
       newEmptyMVar, putMVar, takeMVar)

import Network.AMQP (
       Ack(..), QueueOpts(..), 
       Connection, Channel, Message, Envelope, 
       closeConnection, openConnection, openChannel, msgBody, 
       ackEnv, consumeMsgs, bindQueue, declareQueue)

import qualified Data.ByteString.Lazy.Char8 as B

type QueueName = String
type Log = MVar String
type Logger = String -> IO ()

queueNames :: [QueueName]
queueNames = ["warning", "info", "error"]

-- | The parameters used to connect to a RabbitMQ server
data ConnParams = ConnParams {
     host        :: String,
     virtualHost :: String,
     userName    :: String,
     password    :: String
} deriving Show

-- | Given a log value, returns a function that writes strings
--   to that log value.
mkLogger :: Log -> String -> IO ()
mkLogger = putMVar

-- | Parses the command line to get the connection parameters for
--   RabbitMQ.
parseArgs :: IO (Either String ConnParams)
parseArgs = do
          args <- getArgs
          case args of
               [h, v, n, p] -> return $ Right $ ConnParams h v n p
               _            -> return $ Left $ "invalid arguments"

-- | Prints a usage message to let the user know how to run this
-- program.
usage :: String -> IO ()
usage error = do
      putStrLn $ "error: " ++ error
      putStrLn "usage: runghc RabbitTrail.hs HOST VIRTUAL_HOST USER_NAME PASSWORD"
      exitWith $ ExitFailure 1

-- | Connects to RabbitMQ and reads messages off of the info, warning,
-- and error queues.
run :: ConnParams -> Logger -> IO ()
run p logger = do 
    connResult <- try $ connect p
    case connResult of
         Right((conn,chan)) -> do
             logAllMessages chan logger queueNames
             logger "Press ENTER to quit..."
             getLine
             logger "Closing Connection"
             closeConnection conn
         Left(a :: IOException) -> do
             logger "Could not connect"

-- | Connect to RabbitMQ and return both the connection and the
-- channel.  The connection needs to be closed later and the channel
-- is used to operate on queues.
connect :: ConnParams -> IO (Connection, Channel)
connect p = do 
    conn <- openConnection (host p) (virtualHost p) (userName p) (password p)
    chan <- openChannel conn
    return (conn, chan)

anonymousQueue = QueueOpts "" False False True True

-- | Start reading messages off of the info, warning, and error queues
logAllMessages ::Channel -> Logger -> [QueueName] -> IO ()
logAllMessages chan logger = do
                mapM_ (logMessages logger chan)

-- | Log all messages found on the specified queue.
logMessages :: Logger -> Channel -> QueueName -> IO ()
logMessages logger chan queueName = do
             logger $ "listening to " ++ queueName ++ " queue."
             (queue, _, _) <- declareQueue chan anonymousQueue
             bindQueue chan queue "amq.rabbitmq.log" queueName
             consumeMsgs chan queue Ack (onMessage logger queueName)
             return ()
              
-- | Called whenever a message is read from a queue
onMessage :: Logger -> QueueName -> (Message, Envelope)  -> IO ()
onMessage logger queueName (msg, envelope) = do
         logger $ queueName ++ ": " ++ (B.unpack $ msgBody msg)
         ackEnv envelope

-- | The thread that receives log messages and writes them to the
-- console.
outputThread :: Log -> IO ()
outputThread log = do
             msg <- takeMVar log
             hPutStrLn stdout msg
             hFlush stdout             
             outputThread log

-- | Main entry point into the program
main = do
     args <- parseArgs 
     case args of
          -- Could not parse the command line arguments.
          Left(error) -> usage error
          Right(connParams) -> do
             -- create an MVar used to synchronize access to the
             -- console so that console output doesn't get jumbled.
             log <- newEmptyMVar :: IO (Log)
             let logger = mkLogger log 
             -- outputThread reads from the log MVar so it needs
             -- direct access to the MVar.
             forkIO (outputThread log)
             -- run (and the functions it calls) just writes to the
             -- log MVar so passing the logger function is enough.
             run connParams logger
