using System;
using System.Collections.Generic;
using System.Configuration;
using MySql.Data.MySqlClient;
using System.Data;

namespace com.w3cnet.Net
{
    /// <summary>
    /// MySQL数据库工具类
    /// </summary>
    public abstract class MySQLUtil
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public static readonly string ConnStr = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;

		/// <summary>
        /// 执行语句并返回影响的行数
        /// </summary>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>影响的行数</returns>
        public static int ExecuteNonQuery(string cmdText, params MySqlParameter[] parameters)
        {
            using (var conn = new MySqlConnection(ConnStr))
            {
                return ExecuteNonQuery(conn, CommandType.Text, cmdText, parameters);
            }
        }
		
        /// <summary>
        /// 执行语句并返回影响的行数
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>影响的行数</returns>
        public static int ExecuteNonQuery(string connStr, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var conn = new MySqlConnection(connStr))
            {
                return ExecuteNonQuery(conn, cmdType, cmdText, parameters);
            }
        }

        /// <summary>
        /// 执行语句并返回影响的行数
        /// </summary>
        /// <param name="conn">数据库连接</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>影响的行数</returns>
        public static int ExecuteNonQuery(MySqlConnection conn, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var cmd = new MySqlCommand())
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, parameters);

                var val = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();

                return val;
            }
        }

        /// <summary>
        /// 执行语句并返回影响的行数
        /// </summary>
        /// <param name="trans">事务</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>影响的行数</returns>
        public static int ExecuteNonQuery(MySqlTransaction trans, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var cmd = new MySqlCommand())
            {
                PrepareCommand(cmd, trans.Connection, trans, cmdType, cmdText, parameters);
                var val = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();

                return val;
            }
        }

        /// <summary>
        /// 执行语句并返回DataReader对象
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>执行语句并返回DataReader对象</returns>
        /// <remarks>P.S：DataReader使用后要手动释放</remarks>
        public static MySqlDataReader ExecuteReader(string connStr, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            var cmd = new MySqlCommand();
            var conn = new MySqlConnection(connStr);

            try
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, parameters);
                var dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();

                return dr;
            }
            catch
            {
                conn.Close();
                throw;
            }
        }

        /// <summary>
        /// 执行语句并返回DataSet对象
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static DataSet ExecuteDataSet(string cmdText, params MySqlParameter[] parameters)
        {
            return ExecuteDataSet(ConnStr, CommandType.Text, cmdText, parameters);
        }

        /// <summary>
        /// 执行语句并返回DataSet对象
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>执行语句并返回DataSet对象</returns>
        /// <returns></returns>
        public static DataSet ExecuteDataSet(string connStr, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var conn = new MySqlConnection(connStr))
            {
                return ExecuteDataSet(conn, cmdType, cmdText, parameters);
            }
        }

        /// <summary>
        /// 执行语句并返回DataSet对象
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>执行语句并返回DataSet对象</returns>
        /// <returns></returns>
        public static DataSet ExecuteDataSet(MySqlConnection conn, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var cmd = new MySqlCommand())
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, parameters);
                using (var da = new MySqlDataAdapter(cmd))
                {
                    var ds = new DataSet();
                    try
                    {
                        da.Fill(ds, "ds");
                        cmd.Parameters.Clear();
                    }
                    catch (MySqlException ex)
                    {
                        throw new Exception(ex.Message);
                    }
                    return ds;
                }
            }
        }

        /// <summary>
        /// 执行语句并返回一个对象
        /// </summary>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>返回一个object对象，对象中只包含一列。</returns>
        public static object ExecuteScalar(string cmdText, params MySqlParameter[] parameters)
        {
            return ExecuteScalar(ConnStr, CommandType.Text, cmdText, parameters);
        }

        /// <summary>
        /// 执行语句并返回一个对象
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>返回一个object对象，对象中只包含一列。</returns>
        public static object ExecuteScalar(string connStr, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var conn = new MySqlConnection(connStr))
            {
                return ExecuteScalar(conn, cmdType, cmdText, parameters);
            }
        }

        /// <summary>
        /// 执行语句并返回一个对象
        /// </summary>
        /// <param name="conn">数据库连接</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="parameters">参数集</param>
        /// <returns>返回一个object对象，对象中只包含一列。</returns>
        public static object ExecuteScalar(MySqlConnection conn, CommandType cmdType, string cmdText, params MySqlParameter[] parameters)
        {
            using (var cmd = new MySqlCommand())
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, parameters);
                var val = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return val;
            }
        }

        /// <summary>
        /// 为执行准备命令对象
        /// </summary>
        /// <param name="cmd">command 对象</param>
        /// <param name="conn">连接对象</param>
        /// <param name="trans">事务对象</param>
        /// <param name="cmdType">命令类型</param>
        /// <param name="cmdText">命令字符串</param>
        /// <param name="cmdParms">参数集</param>
        private static void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, CommandType cmdType, string cmdText, IEnumerable<MySqlParameter> parameters)
        {

            if (conn.State != ConnectionState.Open)
                conn.Open();

            cmd.Connection = conn;
            cmd.CommandText = cmdText;

            if (trans != null)
                cmd.Transaction = trans;

            cmd.CommandType = cmdType;

            if (parameters == null) return;
            foreach (var parm in parameters)
                cmd.Parameters.Add(parm);
        }
    }
}