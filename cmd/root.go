/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/highercomve/s3-check/lib"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "storage-checker",
	Short: "Check object in a s3 storage",
	Long:  "Check object in a s3 storage",
	RunE:  lib.CheckStorage,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.Flags().StringP("key", "k", "", "s3 ACCESS_KEY")
	rootCmd.Flags().StringP("secret", "s", "", "s3 SECRET")
	rootCmd.Flags().StringP("region", "r", "", "s3 REGION")
	rootCmd.Flags().StringP("bucket", "b", "", "s3 BUCKET")
	rootCmd.Flags().StringP("endpoint", "e", "", "s3 ENDPOINT")
	rootCmd.Flags().StringP("database", "d", "", "database url")
	rootCmd.Flags().StringP("collection", "c", "", "database collection")
	rootCmd.Flags().StringP("connection", "m", "", "database connection")
	rootCmd.Flags().BoolP("printall", "a", false, "Print all values in the database")

	viper.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))
	viper.BindPFlag("secret", rootCmd.PersistentFlags().Lookup("secret"))
	viper.BindPFlag("region", rootCmd.PersistentFlags().Lookup("region"))
	viper.BindPFlag("bucket", rootCmd.PersistentFlags().Lookup("bucket"))
	viper.BindPFlag("endpoint", rootCmd.PersistentFlags().Lookup("endpoint"))
	viper.BindPFlag("database", rootCmd.PersistentFlags().Lookup("database"))
	viper.BindPFlag("collection", rootCmd.PersistentFlags().Lookup("collection"))
	viper.BindPFlag("connection", rootCmd.PersistentFlags().Lookup("connection"))
	viper.BindPFlag("printall", rootCmd.PersistentFlags().Lookup("printall"))
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".cobra" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".cobra")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
