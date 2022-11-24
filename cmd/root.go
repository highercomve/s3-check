/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"
	"path"

	"github.com/highercomve/s3-check/lib"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "s3-check",
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

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cobra.yaml)")
	rootCmd.PersistentFlags().String("cpuprofile", "", "CPU profiling")
	rootCmd.Flags().Int64("ratelimit", 0, "rate limit per minute to search for objects in s3")
	rootCmd.Flags().Int64P("limit", "l", 100, "Request limit")
	rootCmd.Flags().StringP("key", "k", "", "s3 ACCESS_KEY")
	rootCmd.Flags().StringP("secret", "s", "", "s3 SECRET")
	rootCmd.Flags().StringP("region", "r", "", "s3 REGION")
	rootCmd.Flags().StringP("bucket", "b", "", "s3 BUCKET")
	rootCmd.Flags().StringP("endpoint", "e", "", "s3 ENDPOINT")
	rootCmd.Flags().StringP("database", "d", "", "database name")
	rootCmd.Flags().StringP("collection", "c", "", "database collection")
	rootCmd.Flags().StringP("connection", "m", "", "database connection url")
	rootCmd.Flags().BoolP("printall", "a", false, "Print all values in the database")
	rootCmd.Flags().BoolP("stream", "t", false, "Stream output instead of waiting")

	viper.BindPFlags(rootCmd.Flags())
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		p, err := os.Executable()
		if err != nil {
			fmt.Println(err)
			return
		}
		cobra.CheckErr(err)

		// Search config in home directory with name ".config"
		viper.AddConfigPath(home)
		viper.AddConfigPath(path.Dir(p))
		viper.SetConfigType("yaml")
		viper.SetConfigName(".config")
	}

	viper.AutomaticEnv()
	viper.ReadInConfig()
}
