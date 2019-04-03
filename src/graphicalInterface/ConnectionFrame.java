package graphicalInterface;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.border.EmptyBorder;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import graphicalInterface.draw.DrawIndex;
import graphicalInterface.draw.DrawSchemaNavigator;
import graphicalInterface.images.ImagensController;

import javax.swing.ImageIcon;
import java.awt.event.MouseAdapter;

public class ConnectionFrame extends JFrame {
	
	
	private static ConnectionFrame connectionFrame;
	
	public static ConnectionFrame getInstance(){
		if(connectionFrame == null){
			connectionFrame = new ConnectionFrame();
		}
		return connectionFrame;
	}


	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField usertextField;
	private JPasswordField passwordField;
	private InitialFrame initialFrame;
	
	public void InterfaceSystem() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (UnsupportedLookAndFeelException e) {

		} catch (ClassNotFoundException e) {

		} catch (InstantiationException e) {

		} catch (IllegalAccessException e) {

		}
	}
	
	public ConnectionFrame() {
		InterfaceSystem();
		
		this.addWindowListener(new WindowListener() {
			
			public void windowOpened(WindowEvent e){}
			public void windowIconified(WindowEvent e){}
			public void windowDeiconified(WindowEvent e){}
			public void windowDeactivated(WindowEvent e) {}
			public void windowClosed(WindowEvent e) {}
			public void windowActivated(WindowEvent e) {}
			public void windowClosing(WindowEvent e) {
				Kernel.stop();
				System.exit(0);		
			}
		});
		
		
		
		setIconImage(ImagensController.FRAME_ICON_CONNECTION);
		setTitle("Connection");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 480, 423);
		contentPane = new JPanel();
		contentPane.setBackground(Color.WHITE);
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		GridBagLayout gbl_contentPane = new GridBagLayout();
		gbl_contentPane.columnWidths = new int[]{0, 0};
		gbl_contentPane.rowHeights = new int[]{0, 0, 0, 0};
		gbl_contentPane.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_contentPane.rowWeights = new double[]{0.0, 1.0, 0.0, Double.MIN_VALUE};
		contentPane.setLayout(gbl_contentPane);
		
		JPanel ppanel = new JPanel();
		ppanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_ppanel = new GridBagConstraints();
		gbc_ppanel.gridheight = 3;
		gbc_ppanel.insets = new Insets(0, 0, 5, 0);
		gbc_ppanel.fill = GridBagConstraints.BOTH;
		gbc_ppanel.gridx = 0;
		gbc_ppanel.gridy = 0;
		contentPane.add(ppanel, gbc_ppanel);
		GridBagLayout gbl_ppanel = new GridBagLayout();
		gbl_ppanel.columnWidths = new int[]{0, 0, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_ppanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_ppanel.columnWeights = new double[]{0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_ppanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		ppanel.setLayout(gbl_ppanel);
		
		JLabel lblUser = new JLabel("User");
		GridBagConstraints gbc_lblUser = new GridBagConstraints();
		gbc_lblUser.anchor = GridBagConstraints.WEST;
		gbc_lblUser.insets = new Insets(0, 0, 5, 5);
		gbc_lblUser.gridx = 2;
		gbc_lblUser.gridy = 4;
		ppanel.add(lblUser, gbc_lblUser);
		
		usertextField = new JTextField();
		usertextField.setText("admin");
		usertextField.setHorizontalAlignment(SwingConstants.TRAILING);
		GridBagConstraints gbc_usertextField = new GridBagConstraints();
		gbc_usertextField.gridwidth = 2;
		gbc_usertextField.insets = new Insets(0, 0, 5, 5);
		gbc_usertextField.fill = GridBagConstraints.HORIZONTAL;
		gbc_usertextField.gridx = 10;
		gbc_usertextField.gridy = 4;
		ppanel.add(usertextField, gbc_usertextField);
		usertextField.setColumns(10);
		
		JLabel lblPassword = new JLabel("Password");
		
		GridBagConstraints gbc_lblPassword = new GridBagConstraints();
		gbc_lblPassword.anchor = GridBagConstraints.WEST;
		gbc_lblPassword.insets = new Insets(0, 0, 5, 5);
		gbc_lblPassword.gridx = 2;
		gbc_lblPassword.gridy = 5;
		ppanel.add(lblPassword, gbc_lblPassword);
		
		passwordField = new JPasswordField();
		passwordField.setHorizontalAlignment(SwingConstants.TRAILING);
		GridBagConstraints gbc_passwordField = new GridBagConstraints();
		gbc_passwordField.gridwidth = 2;
		gbc_passwordField.insets = new Insets(0, 0, 5, 5);
		gbc_passwordField.fill = GridBagConstraints.HORIZONTAL;
		gbc_passwordField.gridx = 10;
		gbc_passwordField.gridy = 5;
		ppanel.add(passwordField, gbc_passwordField);
		
		JLabel lblSchema = new JLabel("Schema");
		GridBagConstraints gbc_lblSchema = new GridBagConstraints();
		gbc_lblSchema.anchor = GridBagConstraints.WEST;
		gbc_lblSchema.insets = new Insets(0, 0, 5, 5);
		gbc_lblSchema.gridx = 2;
		gbc_lblSchema.gridy = 6;
		ppanel.add(lblSchema, gbc_lblSchema);
		
		JComboBox<String> schemaComboBox = new JComboBox<String>((Vector<String>)Kernel.getSchemasNames());
		schemaComboBox.setMaximumRowCount(12);
		GridBagConstraints gbc_schemaComboBox = new GridBagConstraints();
		gbc_schemaComboBox.gridwidth = 2;
		gbc_schemaComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_schemaComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_schemaComboBox.gridx = 10;
		gbc_schemaComboBox.gridy = 6;
		ppanel.add(schemaComboBox, gbc_schemaComboBox);
		
		schemaComboBox.addMouseListener(new MouseListener() {
			int lastSize = 0;
			public void mouseReleased(MouseEvent e) {}	
			public void mousePressed(MouseEvent e) {}
			public void mouseExited(MouseEvent e) {}
			public void mouseEntered(MouseEvent e) {
				
			}
			
			public void mouseClicked(MouseEvent e) {
				Vector<String> schemas = (Vector<String>)Kernel.getSchemasNames();
			

				if(lastSize != 0 && lastSize != schemas.size()){
					schemaComboBox.setModel(new DefaultComboBoxModel<>(schemas));					
				}
			
				lastSize = schemas.size();
			}
		});
		
		
		JButton buttonStart = new JButton("Connect");
		GridBagConstraints gbc_buttonStart = new GridBagConstraints();
		gbc_buttonStart.anchor = GridBagConstraints.EAST;
		gbc_buttonStart.gridwidth = 5;
		gbc_buttonStart.insets = new Insets(0, 0, 5, 5);
		gbc_buttonStart.gridx = 7;
		gbc_buttonStart.gridy = 7;
		ppanel.add(buttonStart, gbc_buttonStart);
		
		JLabel separator = new JLabel("       ");
		GridBagConstraints gbc_separator = new GridBagConstraints();
		gbc_separator.insets = new Insets(0, 0, 5, 5);
		gbc_separator.gridx = 0;
		gbc_separator.gridy = 9;
		ppanel.add(separator, gbc_separator);
		
		JLabel lblSchemaExplorer = new JLabel("   Schema Explorer");
		lblSchemaExplorer.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				 	JComponent treeview =  DrawSchemaNavigator.create();
			        JFrame frame = new JFrame("Schema Explorer");
			        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
			        frame.setExtendedState(JFrame.MAXIMIZED_BOTH);
			        frame.setIconImage(ImagensController.FRAME_ICON_DATABASE_EYE);
			        frame.setContentPane(treeview);
			        frame.pack();
			        frame.setVisible(true);
			}
		});
		
		JLabel logo = new JLabel("");
		logo.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/seal_db_logo_white_low.png")));
		GridBagConstraints gbc_logo = new GridBagConstraints();
		gbc_logo.gridheight = 8;
		gbc_logo.insets = new Insets(0, 0, 5, 5);
		gbc_logo.gridx = 11;
		gbc_logo.gridy = 9;
		ppanel.add(logo, gbc_logo);
		lblSchemaExplorer.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/database_eye.png")));
		GridBagConstraints gbc_lblSchemaExplorer = new GridBagConstraints();
		gbc_lblSchemaExplorer.anchor = GridBagConstraints.WEST;
		gbc_lblSchemaExplorer.insets = new Insets(0, 0, 5, 5);
		gbc_lblSchemaExplorer.gridx = 2;
		gbc_lblSchemaExplorer.gridy = 10;
		ppanel.add(lblSchemaExplorer, gbc_lblSchemaExplorer);
		
		JLabel lblBufferManager = new JLabel("   Buffer Manager");
		lblBufferManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if(initialFrame.getBufferFrame().isVisible()){
					initialFrame.hideBufferFrame();
				}else{
					initialFrame.showBufferFrame();
				}
			}
		});
		
		lblBufferManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/buffer.png")));
		GridBagConstraints gbc_lblBufferManager = new GridBagConstraints();
		gbc_lblBufferManager.gridwidth = 9;
		gbc_lblBufferManager.anchor = GridBagConstraints.WEST;
		gbc_lblBufferManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblBufferManager.gridx = 2;
		gbc_lblBufferManager.gridy = 11;
		ppanel.add(lblBufferManager, gbc_lblBufferManager);
		
		JLabel lblRecoveryManager = new JLabel("   Recovery Manager");
		lblRecoveryManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if(initialFrame.getRecoveryFrame().isVisible()){
					initialFrame.hideRecoveryFrame();
				}else{
					initialFrame.showRecoveryFrame();
				}
			}
		});
		
		JLabel lblTransactionManager = new JLabel("   Transaction Manager");
		lblTransactionManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if(initialFrame.getTransactionFrame().isVisible()){
					initialFrame.hideTransactionFrame();					
				}else{
					initialFrame.showTransactionFrame();
				}
			}
		});
		lblTransactionManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/transaction.png")));
		GridBagConstraints gbc_lblTransactionManager = new GridBagConstraints();
		gbc_lblTransactionManager.gridwidth = 9;
		gbc_lblTransactionManager.anchor = GridBagConstraints.WEST;
		gbc_lblTransactionManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblTransactionManager.gridx = 2;
		gbc_lblTransactionManager.gridy = 12;
		ppanel.add(lblTransactionManager, gbc_lblTransactionManager);
		lblRecoveryManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/recoveryM.png")));
		GridBagConstraints gbc_lblRecoveryManager = new GridBagConstraints();
		gbc_lblRecoveryManager.gridwidth = 9;
		gbc_lblRecoveryManager.anchor = GridBagConstraints.WEST;
		gbc_lblRecoveryManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblRecoveryManager.gridx = 2;
		gbc_lblRecoveryManager.gridy = 13;
		ppanel.add(lblRecoveryManager, gbc_lblRecoveryManager);
		
		JLabel lblIndexManager = new JLabel("   Index View (Tests)");
		lblIndexManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				DrawIndex.open();
			}
		});
		lblIndexManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/index_view.png")));
		GridBagConstraints gbc_lblIndexManager = new GridBagConstraints();
		gbc_lblIndexManager.gridwidth = 9;
		gbc_lblIndexManager.anchor = GridBagConstraints.WEST;
		gbc_lblIndexManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblIndexManager.gridx = 2;
		gbc_lblIndexManager.gridy = 14;
		ppanel.add(lblIndexManager, gbc_lblIndexManager);
		
		JLabel distu = new JLabel("   Nodes Connections (Tests)");
		distu.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
			
				if(initialFrame.getNodeConnectionFrame().isVisible()){
					initialFrame.hideNodeConnectionFrame();					
				}else{
					initialFrame.showNodeConnectionFrame();
				}
				
			}
		});
		distu.setHorizontalAlignment(SwingConstants.LEFT);
		distu.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/distributed.png")));
		GridBagConstraints gbc_distu = new GridBagConstraints();
		gbc_distu.anchor = GridBagConstraints.WEST;
		gbc_distu.gridwidth = 9;
		gbc_distu.insets = new Insets(0, 0, 5, 5);
		gbc_distu.gridx = 2;
		gbc_distu.gridy = 16;
		ppanel.add(distu, gbc_distu);
		
		initialFrame = new InitialFrame();
		buttonStart.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent arg0) {
				
				
				DBConnection connection = 
						DistributedTransactionManagerController.getInstance().
						getLocalConnection
						(schemaComboBox.getSelectedItem().toString(), usertextField.getText(), new String(passwordField.getPassword()));
				
				initialFrame.addPlanLayer(connection);
			}
		});
		
		this.setVisible(true);
	}
	
	public void connectAndShowSQL(String schema, String sqlInitial){
		DBConnection connection = 
				DistributedTransactionManagerController.getInstance().
				getLocalConnection
				(schema, usertextField.getText(), new String(passwordField.getPassword()));
		
		initialFrame.addPlanLayer(connection,sqlInitial);
	}

}
